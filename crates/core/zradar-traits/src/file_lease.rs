//! In-process file read leases.
//!
//! When a reader resolves a set of `file_list` rows to local paths and hands
//! them to a query engine, the underlying files can be relocated (e.g. local →
//! S3 by a mover) or physically removed (by the reclaimer) at the same time.
//! A long-running scan racing a delete can otherwise produce partial results
//! or hard scan errors.
//!
//! The [`FileLeaseRegistry`] is a process-local refcount keyed by
//! `file_list.id`. Readers acquire a lease for every file they are about to
//! scan; the lease is dropped (RAII) when the scan future returns. The single
//! physical-deletion chokepoint (the reclaimer) consults
//! [`FileLeaseRegistry::is_leased`] before removing a file and skips any that
//! still have a non-zero refcount, letting the next tick retry.
//!
//! This lives in `zradar-traits` rather than the storage crate so that both
//! the reader (`zradar-parquet`) and the reclaimer (`zradar-retention`) can
//! share one registry without a crate-dependency cycle.
//!
//! It is intentionally in-process only. The architecture review (§6.8) calls
//! out a future cluster-wide variant via Postgres notifications or NATS; that
//! is left as a follow-up once ingest/query/compaction roles are split across
//! processes.

use std::sync::Arc;

use dashmap::DashMap;

/// Reference-counted, in-process registry of file IDs currently being read.
///
/// Cheap to clone (one `Arc` bump). Safe to share across all readers, the
/// FileMover, and the FileReclaimer.
#[derive(Default)]
pub struct FileLeaseRegistry {
    counts: DashMap<i64, u64>,
}

impl FileLeaseRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            counts: DashMap::new(),
        }
    }

    /// Acquire a lease on each `file_id`. Returns one RAII guard per file —
    /// drop the `Vec<FileLease>` to release them all.
    ///
    /// The order of the returned vec matches the order of `file_ids`.
    pub fn acquire(self: &Arc<Self>, file_ids: &[i64]) -> Vec<FileLease> {
        let mut leases = Vec::with_capacity(file_ids.len());
        for &id in file_ids {
            *self.counts.entry(id).or_insert(0) += 1;
            leases.push(FileLease {
                registry: self.clone(),
                file_id: id,
            });
        }
        leases
    }

    /// Returns true if any reader currently holds a lease on `file_id`.
    ///
    /// The reclaimer (and the FileMover) call this before move/delete and skip
    /// leased files. The next tick retries.
    pub fn is_leased(&self, file_id: i64) -> bool {
        self.counts.get(&file_id).is_some_and(|c| *c.value() > 0)
    }

    /// Total number of currently leased files (for tests/metrics).
    pub fn active_lease_count(&self) -> usize {
        self.counts
            .iter()
            .filter(|entry| *entry.value() > 0)
            .count()
    }

    fn release(&self, file_id: i64) {
        let mut should_remove = false;
        if let Some(mut entry) = self.counts.get_mut(&file_id) {
            let v = entry.value_mut();
            *v = v.saturating_sub(1);
            should_remove = *v == 0;
        }
        if should_remove {
            // Clean up the slot once nobody is leasing it. Done outside the
            // `get_mut` scope to avoid deadlocking the DashMap shard.
            self.counts.remove_if(&file_id, |_, count| *count == 0);
        }
    }
}

/// RAII guard returned by [`FileLeaseRegistry::acquire`]. Releases the lease
/// when dropped.
///
/// `Vec<FileLease>` is the normal shape: keep all leases alive for the
/// duration of one scan, then let the vec drop on function exit.
pub struct FileLease {
    registry: Arc<FileLeaseRegistry>,
    file_id: i64,
}

impl FileLease {
    /// The file ID this lease protects.
    pub fn file_id(&self) -> i64 {
        self.file_id
    }
}

impl Drop for FileLease {
    fn drop(&mut self) {
        self.registry.release(self.file_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_one_lease_per_file_id() {
        let registry = Arc::new(FileLeaseRegistry::new());
        let leases = registry.acquire(&[1, 2, 3]);
        assert_eq!(leases.len(), 3);
        assert_eq!(leases[0].file_id(), 1);
        assert_eq!(leases[1].file_id(), 2);
        assert_eq!(leases[2].file_id(), 3);
    }

    #[test]
    fn is_leased_reports_active_leases() {
        let registry = Arc::new(FileLeaseRegistry::new());
        assert!(!registry.is_leased(42));

        let leases = registry.acquire(&[42]);
        assert!(registry.is_leased(42));
        assert!(!registry.is_leased(43));

        drop(leases);
        assert!(!registry.is_leased(42), "lease should drop on Vec drop");
    }

    #[test]
    fn multiple_concurrent_leases_on_same_file_all_release() {
        let registry = Arc::new(FileLeaseRegistry::new());
        let l1 = registry.acquire(&[100]);
        let l2 = registry.acquire(&[100]);
        let l3 = registry.acquire(&[100]);
        assert!(registry.is_leased(100));

        drop(l1);
        assert!(registry.is_leased(100), "still 2 active");
        drop(l2);
        assert!(registry.is_leased(100), "still 1 active");
        drop(l3);
        assert!(!registry.is_leased(100), "all released");
    }

    #[test]
    fn dropping_partial_vec_releases_only_dropped_files() {
        // Acquire 3 files, drop the middle one only by splitting the vec.
        let registry = Arc::new(FileLeaseRegistry::new());
        let mut leases = registry.acquire(&[1, 2, 3]);
        let _middle = leases.remove(1);

        assert!(registry.is_leased(1));
        assert!(registry.is_leased(2), "middle lease still held");
        assert!(registry.is_leased(3));

        drop(_middle);
        assert!(registry.is_leased(1));
        assert!(!registry.is_leased(2));
        assert!(registry.is_leased(3));
    }

    #[test]
    fn active_lease_count_reflects_distinct_files() {
        let registry = Arc::new(FileLeaseRegistry::new());
        let _a = registry.acquire(&[1, 2]);
        let _b = registry.acquire(&[2, 3]); // file 2 leased twice
        assert_eq!(registry.active_lease_count(), 3);
        drop(_a);
        // After dropping _a: file 1 released (count 0), file 2 still has _b lease.
        assert_eq!(registry.active_lease_count(), 2);
    }

    #[test]
    fn release_of_unknown_file_is_silent() {
        // Construct a lease pointing at a file ID that was never acquired (via
        // direct struct construction in a test). Dropping it must not panic.
        let registry = Arc::new(FileLeaseRegistry::new());
        let phantom = FileLease {
            registry: registry.clone(),
            file_id: 99999,
        };
        drop(phantom);
        // No panic, no entry left behind.
        assert!(!registry.is_leased(99999));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquire_and_release_is_race_free() {
        // Hammer the registry from many tasks; assert no panics and that the
        // final state is "no leases held."
        let registry = Arc::new(FileLeaseRegistry::new());
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let r = registry.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..1000i64 {
                    let _l = r.acquire(&[i % 32]);
                    tokio::task::yield_now().await;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(registry.active_lease_count(), 0);
    }
}
