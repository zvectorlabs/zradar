/// WAL janitor: deletes sealed segments that have been fully flushed.
///
/// Runs periodically (5× the flush interval). Never touches the active segment.
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::Wal;
use crate::checkpoint::CheckpointStore;
use crate::segment;

/// Background task that garbage-collects old WAL segments.
pub struct WalJanitor {
    wal: Arc<Wal>,
    checkpoint_store: Arc<CheckpointStore>,
    interval: Duration,
}

impl WalJanitor {
    /// Create a janitor. `flush_interval_ms` is the flusher interval; the janitor
    /// runs at 5× that interval.
    pub fn new(
        wal: Arc<Wal>,
        checkpoint_store: Arc<CheckpointStore>,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            wal,
            checkpoint_store,
            interval: Duration::from_millis(flush_interval_ms * 5),
        }
    }

    /// Run the janitor loop until cancellation.
    pub async fn run(self, cancel: CancellationToken) {
        info!("WAL janitor started");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("WAL janitor shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.interval) => {}
            }

            if let Err(e) = self.cleanup_once().await {
                error!(error = %e, "WAL janitor cleanup failed");
            }
        }
    }

    /// Single cleanup pass: delete segments whose max offset ≤ checkpoint.
    async fn cleanup_once(&self) -> anyhow::Result<()> {
        let checkpoint = match self.checkpoint_store.load()? {
            Some(cp) => cp,
            None => return Ok(()), // nothing flushed yet
        };
        let segments = segment::list_segments(self.wal.dir())?;
        let active_id = self.wal.active_segment_id().await;

        let mut deleted_count = 0u32;

        for &seg_id in &segments {
            // Never delete the active segment
            if seg_id >= active_id {
                continue;
            }

            // Only delete segments fully before the checkpoint
            if seg_id < checkpoint.last_flushed_segment_id {
                let path = segment::segment_path(self.wal.dir(), seg_id);
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        deleted_count += 1;
                        debug!(segment_id = seg_id, "deleted flushed segment");
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Already deleted
                    }
                    Err(e) => {
                        warn!(segment_id = seg_id, error = %e, "failed to delete segment");
                    }
                }
            }
        }

        if deleted_count > 0 {
            info!(deleted = deleted_count, "WAL janitor cleaned segments");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use crate::checkpoint::{Checkpoint, CheckpointStore};
    use crate::config::WalConfig;
    use crate::record::{SignalType, WalRecord};
    use bytes::Bytes;
    use tempfile::TempDir;

    fn make_record() -> WalRecord {
        WalRecord {
            signal_type: SignalType::Log,
            workspace_id: WorkspaceId::new(),
            arrival_timestamp_ns: 1_700_000_000_000_000_000,
            assigned_offset: 0,
            payload: Bytes::from(vec![0xDD; 64]),
        }
    }

    #[tokio::test]
    async fn test_janitor_deletes_flushed_sealed_segments() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 400, // very small to force rotation quickly
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        // Write enough records to create multiple segments
        for _ in 0..5 {
            let h = wal.append(make_record()).await.unwrap();
            h.durable().await.unwrap();
        }

        let segments_before = segment::list_segments(tmp.path()).unwrap();
        assert!(
            segments_before.len() >= 2,
            "need at least 2 segments for janitor test"
        );

        // Set checkpoint to indicate all segments except the last are flushed
        let active_id = wal.active_segment_id().await;
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));
        checkpoint_store
            .save(&Checkpoint {
                last_flushed_segment_id: active_id,
                last_flushed_offset: 999,
                wal_format_version: 1,
            })
            .unwrap();

        let janitor = WalJanitor::new(wal.clone(), checkpoint_store, 20);
        // Run one cleanup pass
        janitor.cleanup_once().await.unwrap();

        let segments_after = segment::list_segments(tmp.path()).unwrap();
        // Old segments should be deleted; only the active one remains
        assert!(
            segments_after.len() < segments_before.len(),
            "janitor should have deleted some segments"
        );
        assert!(
            segments_after.contains(&active_id),
            "active segment must not be deleted"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_janitor_never_deletes_active() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        let h = wal.append(make_record()).await.unwrap();
        h.durable().await.unwrap();

        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));
        // Checkpoint says everything flushed up to a high offset
        checkpoint_store
            .save(&Checkpoint {
                last_flushed_segment_id: 999,
                last_flushed_offset: 999,
                wal_format_version: 1,
            })
            .unwrap();

        let janitor = WalJanitor::new(wal.clone(), checkpoint_store, 20);
        janitor.cleanup_once().await.unwrap();

        // Active segment should still exist
        let segments = segment::list_segments(tmp.path()).unwrap();
        assert_eq!(segments, vec![0], "active segment 0 must not be deleted");

        cancel.cancel();
    }
}
