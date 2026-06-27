//! CleanupJob — retention *policy* enforcement (soft-delete only).
//!
//! The job queries `file_list` grouped by `(tenant_id, project_id)` and uses
//! `RetentionConfigStore` to determine the per-project cutoff. Files whose
//! `max_ts` falls before the cutoff are marked `deleted=true` in metadata.
//!
//! **This job never unlinks bytes.** Physical removal is centralized in
//! [`FileReclaimer`](crate::file_reclaimer::FileReclaimer), which sweeps
//! `deleted=true` rows lease-aware. Compaction follows the same contract:
//! it soft-deletes merged originals and relies on the reclaimer to reclaim
//! disk/S3 space once no reader holds a lease.
//!
//! `run_now()` executes one cleanup cycle synchronously — useful for tests and
//! admin-triggered cleanups. `run(cancel)` loops on a configurable schedule.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use zradar_models::FileListFilter;
use zradar_traits::FileListRepository;

use crate::config::RetentionConfigStore;

/// Statistics returned after a cleanup (mark-for-deletion) cycle.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct CleanupStats {
    /// Expired files marked `deleted=true` this cycle (not yet physically removed).
    pub files_marked: u64,
    pub projects_processed: u32,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

impl CleanupStats {
    /// Merge reclaim stats into an admin/API response shape.
    ///
    /// `files_deleted` and `bytes_freed` reflect *physical* reclaim work done
    /// by [`FileReclaimer`](crate::file_reclaimer::FileReclaimer), while
    /// `files_marked` reflects this policy job's soft-delete count.
    pub fn with_reclaim(
        mut self,
        reclaim: &crate::file_reclaimer::ReclaimStats,
    ) -> RetentionRunStats {
        RetentionRunStats {
            files_marked: self.files_marked,
            files_deleted: reclaim.files_reclaimed,
            bytes_freed: reclaim.bytes_freed,
            files_skipped_leased: reclaim.files_skipped_leased,
            projects_processed: self.projects_processed,
            errors: {
                self.errors.append(&mut reclaim.errors.clone());
                self.errors
            },
            duration_ms: self.duration_ms.saturating_add(reclaim.duration_ms),
        }
    }
}

/// Combined stats returned by the admin retention run endpoint.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RetentionRunStats {
    pub files_marked: u64,
    /// Physically reclaimed files (FileReclaimer).
    pub files_deleted: u64,
    pub bytes_freed: u64,
    pub files_skipped_leased: u64,
    pub projects_processed: u32,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

/// Background job that soft-deletes expired Parquet files per retention policy.
pub struct CleanupJob {
    file_list_repo: Arc<dyn FileListRepository>,
    config_store: Arc<RetentionConfigStore>,
    /// How often (seconds) the scheduled loop wakes up.
    interval_secs: u64,
}

impl CleanupJob {
    /// Create a new `CleanupJob`.
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        config_store: Arc<RetentionConfigStore>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            config_store,
            interval_secs,
        }
    }

    /// Execute one cleanup cycle and return statistics.
    pub async fn run_now(&self) -> anyhow::Result<CleanupStats> {
        self.run_now_scoped(None, None).await
    }

    /// Execute one cleanup cycle for an optional tenant/project scope.
    pub async fn run_now_scoped(
        &self,
        tenant_id: Option<uuid::Uuid>,
        project_id: Option<uuid::Uuid>,
    ) -> anyhow::Result<CleanupStats> {
        let started = Instant::now();
        let mut stats = CleanupStats::default();

        // Only active files — expired ones get soft-deleted here; the reclaimer
        // handles `deleted=true` rows separately.
        let all_files = self
            .file_list_repo
            .query_files(FileListFilter {
                tenant_id,
                project_id,
                deleted: Some(false),
                ..FileListFilter::default()
            })
            .await?;

        if all_files.is_empty() {
            return Ok(stats);
        }

        let mut by_project: HashMap<(uuid::Uuid, uuid::Uuid), Vec<_>> = HashMap::new();
        for file in all_files {
            by_project
                .entry((file.tenant_id, file.project_id))
                .or_default()
                .push(file);
        }

        stats.projects_processed = by_project.len() as u32;

        for ((tenant_id, project_id), files) in &by_project {
            let cutoff_us = self.config_store.get_cutoff_us(*tenant_id, *project_id);

            let expired: Vec<_> = files.iter().filter(|f| f.max_ts <= cutoff_us).collect();

            if expired.is_empty() {
                continue;
            }

            info!(
                tenant_id = %tenant_id,
                project_id = %project_id,
                count = expired.len(),
                cutoff_us,
                "CleanupJob: marking expired files deleted=true"
            );

            let ids: Vec<i64> = expired.iter().map(|f| f.id).collect();
            if let Err(e) = self.file_list_repo.mark_deleted(&ids).await {
                let msg = format!("mark_deleted failed for project {tenant_id}/{project_id}: {e}");
                error!("{}", msg);
                stats.errors.push(msg);
                continue;
            }

            stats.files_marked += ids.len() as u64;
        }

        stats.duration_ms = started.elapsed().as_millis() as u64;

        info!(
            files_marked = stats.files_marked,
            duration_ms = stats.duration_ms,
            "CleanupJob: cycle complete"
        );

        Ok(stats)
    }

    /// Run the cleanup loop until `cancel` is cancelled.
    pub async fn run(&self, cancel: CancellationToken) {
        info!(interval_secs = self.interval_secs, "CleanupJob started");

        let interval = Duration::from_secs(self.interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("CleanupJob shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_now().await {
                        error!(error = %e, "CleanupJob cycle failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use uuid::Uuid;
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
    };

    struct EmptyRepo;

    #[async_trait::async_trait]
    impl FileListRepository for EmptyRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            Ok(1)
        }
        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(vec![])
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(&self, _: Uuid, _: Uuid) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct MarkRepo {
        files: Vec<FileListEntry>,
        marked: Mutex<Vec<i64>>,
    }

    #[async_trait::async_trait]
    impl FileListRepository for MarkRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            Ok(1)
        }
        async fn query_files(&self, filter: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(self
                .files
                .iter()
                .filter(|f| filter.deleted.is_none_or(|deleted| f.deleted == deleted))
                .cloned()
                .collect())
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, ids: &[i64]) -> anyhow::Result<()> {
            self.marked.lock().unwrap().extend_from_slice(ids);
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(&self, _: Uuid, _: Uuid) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_run_now_no_files_returns_empty_stats() {
        let store = Arc::new(RetentionConfigStore::new(30));
        let job = CleanupJob::new(Arc::new(EmptyRepo), store, 3600);
        let stats = job.run_now().await.unwrap();
        assert_eq!(stats.files_marked, 0);
        assert!(stats.errors.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_marks_expired_without_physical_delete() {
        let store = Arc::new(RetentionConfigStore::new(7));
        let tenant = Uuid::new_v4();
        let project = Uuid::new_v4();
        let repo = Arc::new(MarkRepo {
            files: vec![FileListEntry {
                id: 5,
                tenant_id: tenant,
                project_id: project,
                signal_type: "traces".to_string(),
                stream_name: "svc".to_string(),
                date: "2020/01/01/00".to_string(),
                file_path: "/tmp/old.parquet".to_string(),
                location: "local".to_string(),
                min_ts: 0,
                max_ts: 1,
                records: 1,
                original_size: 100,
                compressed_size: 100,
                deleted: false,
                created_at: 0,
                updated_at: 0,
            }],
            marked: Mutex::new(Vec::new()),
        });
        let job = CleanupJob::new(repo.clone(), store, 3600);
        let stats = job.run_now().await.unwrap();
        assert_eq!(stats.files_marked, 1);
        assert_eq!(repo.marked.lock().unwrap().as_slice(), &[5]);
    }

    #[tokio::test]
    async fn test_cancel_stops_scheduled_loop() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let store = Arc::new(RetentionConfigStore::new(30));
        let job = CleanupJob::new(Arc::new(EmptyRepo), store, 3600);

        cancel_clone.cancel();
        tokio::time::timeout(Duration::from_secs(1), job.run(cancel))
            .await
            .expect("CleanupJob should stop when cancelled");
    }
}
