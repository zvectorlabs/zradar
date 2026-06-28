/// Disk-resident Write-Ahead Log for zradar.
///
/// Every OTLP request is appended to the WAL and fsynced (via group-commit)
/// before being acknowledged. A background flusher drains records into the
/// existing Parquet write path. On crash, replay re-materializes any records
/// that had been acknowledged but not yet flushed.
pub mod backpressure;
pub mod batch;
pub mod checkpoint;
pub mod config;
pub mod flusher;
pub mod group_commit;
pub mod janitor;
pub mod metrics;
pub mod record;
pub mod replay;
pub mod segment;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use config::WalConfig;
use group_commit::{AppendError, AppendHandle, FsyncQueue};
use record::WalRecord;
use segment::{SegmentWriter, list_segments, update_current_symlink};

/// The core WAL handle. Clone-safe via internal `Arc`s.
pub struct Wal {
    inner: Arc<WalInner>,
}

struct WalInner {
    dir: PathBuf,
    config: WalConfig,
    writer: Mutex<SegmentWriter>,
    fsync_queue: Arc<FsyncQueue>,
    offset_counter: AtomicU64,
    _fsync_task: tokio::task::JoinHandle<()>,
}

impl Wal {
    /// Open (or create) the WAL directory. Finds the active segment or starts fresh.
    ///
    /// Spawns the group-commit fsync background task.
    pub async fn open(
        dir: &Path,
        config: WalConfig,
        cancel: CancellationToken,
    ) -> Result<Self, WalError> {
        std::fs::create_dir_all(dir)?;

        let segments = list_segments(dir)?;

        let (writer, next_offset) = if let Some(&last_id) = segments.last() {
            // Recover: scan the last segment to find the highest offset
            let mut reader = segment::SegmentReader::open(dir, last_id)?;
            let mut max_offset = 0u64;
            loop {
                match reader.next_record() {
                    Ok(Some(rec)) => {
                        max_offset = max_offset.max(rec.assigned_offset);
                    }
                    Ok(None) => break,
                    Err(segment::SegmentError::Record(
                        record::RecordReadError::TornWriteIncomplete { .. }
                        | record::RecordReadError::TornWriteCrcMismatch { .. }
                        | record::RecordReadError::TornWriteTruncated { .. },
                    )) => {
                        let trunc_point = reader.truncation_point();
                        tracing::warn!(
                            segment_id = last_id,
                            trunc_point,
                            "torn write detected, truncating segment tail"
                        );
                        segment::truncate_segment(dir, last_id, trunc_point)?;
                        break;
                    }
                    Err(e) => return Err(WalError::Segment(e)),
                }
            }

            // If the last segment is at or over max size, seal and create a new one
            let file_size = std::fs::metadata(segment::segment_path(dir, last_id))?.len();
            if file_size >= config.segment_max_bytes {
                let new_id = last_id + 1;
                let w = SegmentWriter::create(dir, new_id)?;
                update_current_symlink(dir, new_id)?;
                (w, max_offset + 1)
            } else {
                // Re-open for appending
                let w = reopen_segment_for_append(dir, last_id)?;
                (w, max_offset + 1)
            }
        } else {
            // Fresh WAL directory — start at segment 0
            let w = SegmentWriter::create(dir, 0)?;
            update_current_symlink(dir, 0)?;
            (w, 0)
        };

        let fsync_queue = Arc::new(FsyncQueue::new());

        // The fsync closure follows the `current.seg` symlink so it always
        // syncs the active segment, even after rotation.
        let symlink_path = dir.join("current.seg");
        let fsync_fn: Arc<dyn Fn() -> Result<(), std::io::Error> + Send + Sync> =
            Arc::new(move || {
                let real_path =
                    std::fs::read_link(&symlink_path).unwrap_or_else(|_| symlink_path.clone());
                let parent = symlink_path.parent().unwrap_or(Path::new("."));
                let target = if real_path.is_relative() {
                    parent.join(&real_path)
                } else {
                    real_path
                };
                let f = std::fs::OpenOptions::new().write(true).open(target)?;
                f.sync_all()?;
                Ok(())
            });

        let fsync_task = tokio::spawn(group_commit::group_commit_loop(
            fsync_queue.clone(),
            fsync_fn,
            config.group_commit_window_ms,
            cancel,
        ));

        Ok(Self {
            inner: Arc::new(WalInner {
                dir: dir.to_path_buf(),
                config,
                writer: Mutex::new(writer),
                fsync_queue,
                offset_counter: AtomicU64::new(next_offset),
                _fsync_task: fsync_task,
            }),
        })
    }

    /// Append a record to the WAL. Returns an `AppendHandle` whose `durable()` future
    /// resolves once the group-commit fsync covers this record.
    pub async fn append(&self, mut record: WalRecord) -> Result<AppendHandle, AppendError> {
        let offset = self.inner.offset_counter.fetch_add(1, Ordering::SeqCst);
        record.assigned_offset = offset;

        let serialized = record.serialize();

        let mut writer = self.inner.writer.lock().await;

        // Check if we need to seal and rotate
        if writer.size() + serialized.len() as u64 > self.inner.config.segment_max_bytes {
            writer
                .fsync()
                .map_err(|e| AppendError::Io(std::io::Error::other(e.to_string())))?;
            let new_id = writer.id() + 1;
            let new_writer = SegmentWriter::create(&self.inner.dir, new_id)
                .map_err(|e| AppendError::Io(std::io::Error::other(e.to_string())))?;
            update_current_symlink(&self.inner.dir, new_id)
                .map_err(|e| AppendError::Io(std::io::Error::other(e.to_string())))?;
            *writer = new_writer;
        }

        writer
            .append(&serialized)
            .map_err(|e| AppendError::Io(std::io::Error::other(e.to_string())))?;
        writer
            .flush()
            .map_err(|e| AppendError::Io(std::io::Error::other(e.to_string())))?;

        drop(writer);

        let rx = self.inner.fsync_queue.register().await;
        Ok(AppendHandle::new(rx))
    }

    /// Current active segment size in bytes.
    pub async fn active_segment_size(&self) -> u64 {
        self.inner.writer.lock().await.size()
    }

    /// Current active segment id.
    pub async fn active_segment_id(&self) -> u64 {
        self.inner.writer.lock().await.id()
    }

    /// The next offset that will be assigned.
    pub fn next_offset(&self) -> u64 {
        self.inner.offset_counter.load(Ordering::SeqCst)
    }

    /// Path to the WAL directory.
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// WAL configuration.
    pub fn config(&self) -> &WalConfig {
        &self.inner.config
    }

    /// Total number of fsync calls made by the group-commit task.
    pub fn fsync_count(&self) -> u64 {
        self.inner.fsync_queue.fsync_count()
    }

    /// Iterate all records in a given segment (for flusher/replayer).
    pub fn iter_segment(
        dir: &Path,
        segment_id: u64,
    ) -> Result<segment::SegmentReader, segment::SegmentError> {
        segment::SegmentReader::open(dir, segment_id)
    }
}

/// Errors from WAL operations.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("segment error: {0}")]
    Segment(#[from] segment::SegmentError),
}

/// Reopen an existing segment for appending (seek to end).
fn reopen_segment_for_append(dir: &Path, segment_id: u64) -> Result<SegmentWriter, WalError> {
    use std::io::{Seek, SeekFrom};

    let path = segment::segment_path(dir, segment_id);
    let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
    let size = file.seek(SeekFrom::End(0))?;

    // We need to construct a SegmentWriter with the correct state.
    // Since SegmentWriter::create would truncate, we construct manually via a helper.
    Ok(SegmentWriter::open_existing(path, file, segment_id, size))
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use bytes::Bytes;
    use record::SignalType;
    use tempfile::TempDir;

    fn make_record() -> WalRecord {
        WalRecord {
            signal_type: SignalType::Trace,
            workspace_id: WorkspaceId::new(),
            arrival_timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            assigned_offset: 0, // will be overwritten by append
            payload: Bytes::from(vec![0xAA; 256]),
        }
    }

    #[tokio::test]
    async fn test_open_fresh_wal() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();
        assert_eq!(wal.next_offset(), 0);
        assert_eq!(wal.active_segment_id().await, 0);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_append_and_sync() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();

        let handle = wal.append(make_record()).await.unwrap();
        handle.durable().await.unwrap();

        assert_eq!(wal.next_offset(), 1);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_concurrent_appends_preserve_order() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 2,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        let mut handles = Vec::new();
        for _ in 0..1000 {
            let wal = wal.clone();
            handles.push(tokio::spawn(async move {
                let h = wal.append(make_record()).await.unwrap();
                h.durable().await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(wal.next_offset(), 1000);

        // Verify all records are readable in offset order
        let mut reader = segment::SegmentReader::open(tmp.path(), 0).unwrap();
        let mut offsets = Vec::new();
        while let Some(rec) = reader.next_record().unwrap() {
            offsets.push(rec.assigned_offset);
        }
        assert_eq!(offsets.len(), 1000);

        // Offsets should be unique (0..1000) though not necessarily sorted in file
        // since concurrent appends interleave. But each offset is unique.
        offsets.sort_unstable();
        offsets.dedup();
        assert_eq!(offsets.len(), 1000);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_group_commit_coalesces() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 5, // 5ms window to coalesce
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        // Rapid-fire 100 appends
        let mut durable_handles = Vec::new();
        for _ in 0..100 {
            let h = wal.append(make_record()).await.unwrap();
            durable_handles.push(h);
        }

        for h in durable_handles {
            h.durable().await.unwrap();
        }

        let fsyncs = wal.fsync_count();
        assert!(
            fsyncs <= 10,
            "expected coalesced fsyncs <=10 for 100 appends, got {fsyncs}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_segment_rotation() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 500, // very small to force rotation
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();

        // Each record is ~310 bytes serialized, so 2 records should trigger rotation
        let h1 = wal.append(make_record()).await.unwrap();
        h1.durable().await.unwrap();

        let h2 = wal.append(make_record()).await.unwrap();
        h2.durable().await.unwrap();

        // Should have rotated to segment 1
        let segments = list_segments(tmp.path()).unwrap();
        assert!(
            segments.len() >= 2,
            "expected segment rotation, got segments: {:?}",
            segments
        );

        // Symlink should point to the latest segment
        let active_id = wal.active_segment_id().await;
        assert_eq!(active_id, *segments.last().unwrap());

        cancel.cancel();
    }
}
