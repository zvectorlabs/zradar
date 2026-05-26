use std::sync::Arc;
/// WAL flusher: drains committed records into the Parquet write path.
///
/// Runs as a background task, reading records past the checkpoint and
/// forwarding them to `TelemetryWriter`. After each batch completes,
/// the checkpoint advances.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::Wal;
use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::record::WalRecord;
use crate::segment::{self, SegmentReader};

/// Callback trait for the flusher to write records to Parquet.
///
/// This is intentionally decoupled from `TelemetryWriter` so the WAL crate
/// does not depend on `zradar-traits`. The server wiring layer implements this.
#[async_trait::async_trait]
pub trait FlushSink: Send + Sync {
    /// Flush a batch of WAL records. The implementation is responsible for
    /// converting them to Parquet via `TelemetryWriter` and passing the
    /// `max_offset` to the `NewFileListEntry.wal_replay_offset`.
    async fn flush_records(&self, records: &[WalRecord]) -> anyhow::Result<()>;
}

/// Background task that drains WAL segments into Parquet.
pub struct WalFlusher {
    wal: Arc<Wal>,
    sink: Arc<dyn FlushSink>,
    checkpoint_store: Arc<CheckpointStore>,
    flush_interval: Duration,
    seal_notify: Arc<Notify>,
    lag_bytes: Arc<AtomicU64>,
}

impl WalFlusher {
    /// Create a new flusher.
    pub fn new(
        wal: Arc<Wal>,
        sink: Arc<dyn FlushSink>,
        checkpoint_store: Arc<CheckpointStore>,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            wal,
            sink,
            checkpoint_store,
            flush_interval: Duration::from_millis(flush_interval_ms),
            seal_notify: Arc::new(Notify::new()),
            lag_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A handle to the seal-notify, allowing the WAL to wake the flusher
    /// when a segment is sealed.
    pub fn seal_notify(&self) -> Arc<Notify> {
        self.seal_notify.clone()
    }

    /// Current WAL lag in bytes (append highwater − flush checkpoint).
    pub fn lag_bytes(&self) -> u64 {
        self.lag_bytes.load(Ordering::Relaxed)
    }

    /// Run the flusher loop until cancellation.
    pub async fn run(self, cancel: CancellationToken) {
        info!("WAL flusher started");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("WAL flusher shutting down");
                    // Final flush attempt
                    if let Err(e) = self.flush_once().await {
                        error!(error = %e, "final flush on shutdown failed");
                    }
                    break;
                }
                _ = tokio::time::sleep(self.flush_interval) => {}
                _ = self.seal_notify.notified() => {
                    // Small additional coalescing window
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }

            if let Err(e) = self.flush_once().await {
                error!(error = %e, "WAL flush iteration failed, will retry");
            }
        }
    }

    /// Single flush pass: read from checkpoint forward, flush, advance checkpoint.
    async fn flush_once(&self) -> anyhow::Result<()> {
        let checkpoint = self.checkpoint_store.load()?;
        let segments = segment::list_segments(self.wal.dir())?;

        if segments.is_empty() {
            return Ok(());
        }

        let active_seg_id = self.wal.active_segment_id().await;

        // Determine the starting point. `None` checkpoint means nothing has been flushed.
        let start_segment_id = checkpoint
            .as_ref()
            .map_or(0, |cp| cp.last_flushed_segment_id);
        // `None` means all offsets should be flushed; we use a sentinel.
        let already_flushed_offset: Option<u64> =
            checkpoint.as_ref().map(|cp| cp.last_flushed_offset);

        // Process segments from checkpoint forward
        for &seg_id in &segments {
            if seg_id < start_segment_id {
                continue;
            }

            let mut reader = match SegmentReader::open(self.wal.dir(), seg_id) {
                Ok(r) => r,
                Err(e) => {
                    warn!(segment_id = seg_id, error = %e, "cannot open segment for flush");
                    break;
                }
            };

            let mut batch: Vec<WalRecord> = Vec::new();
            let mut max_offset_in_batch: Option<u64> = None;

            loop {
                match reader.next_record() {
                    Ok(Some(rec)) => {
                        if let Some(flushed_offset) = already_flushed_offset
                            && rec.assigned_offset <= flushed_offset
                            && seg_id == start_segment_id
                        {
                            continue;
                        }
                        max_offset_in_batch = Some(
                            max_offset_in_batch
                                .map_or(rec.assigned_offset, |m: u64| m.max(rec.assigned_offset)),
                        );
                        batch.push(rec);
                    }
                    Ok(None) => break,
                    Err(segment::SegmentError::Record(_)) => {
                        debug!(
                            segment_id = seg_id,
                            "torn write at tail during flush, stopping"
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(segment_id = seg_id, error = %e, "error reading segment");
                        break;
                    }
                }
            }

            if let Some(max_offset) = max_offset_in_batch {
                self.sink.flush_records(&batch).await?;

                let new_checkpoint = Checkpoint {
                    last_flushed_segment_id: seg_id,
                    last_flushed_offset: max_offset,
                    wal_format_version: 1,
                };
                self.checkpoint_store.save(&new_checkpoint)?;

                debug!(
                    segment_id = seg_id,
                    flushed_records = batch.len(),
                    max_offset,
                    "WAL batch flushed"
                );
            }

            if seg_id == active_seg_id {
                break;
            }
        }

        // Update lag metric
        let next_offset = self.wal.next_offset();
        let cp = self.checkpoint_store.load()?;
        let flushed = cp.map_or(0, |c| c.last_flushed_offset + 1);
        let lag = next_offset.saturating_sub(flushed);
        self.lag_bytes.store(lag, Ordering::Relaxed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalConfig;
    use crate::record::SignalType;
    use bytes::Bytes;
    use std::sync::Mutex;
    use tempfile::TempDir;

    #[derive(Default)]
    struct MockSink {
        flushed: Mutex<Vec<Vec<WalRecord>>>,
    }

    #[async_trait::async_trait]
    impl FlushSink for MockSink {
        async fn flush_records(&self, records: &[WalRecord]) -> anyhow::Result<()> {
            self.flushed.lock().unwrap().push(records.to_vec());
            Ok(())
        }
    }

    fn make_record(signal: SignalType) -> WalRecord {
        WalRecord {
            signal_type: signal,
            tenant_id: uuid::Uuid::new_v4(),
            project_id: uuid::Uuid::new_v4(),
            arrival_timestamp_ns: 1_700_000_000_000_000_000,
            assigned_offset: 0,
            payload: Bytes::from(vec![0xCC; 64]),
        }
    }

    #[tokio::test]
    async fn test_flusher_drains_records() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        // Append some records
        for _ in 0..5 {
            let h = wal.append(make_record(SignalType::Trace)).await.unwrap();
            h.durable().await.unwrap();
        }

        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        let flusher = WalFlusher::new(wal.clone(), sink.clone(), checkpoint_store.clone(), 50);

        let flusher_cancel = CancellationToken::new();
        let flusher_cancel_clone = flusher_cancel.clone();
        let flusher_handle = tokio::spawn(flusher.run(flusher_cancel_clone));

        // Wait for flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        flusher_cancel.cancel();
        flusher_handle.await.unwrap();

        // Verify records were flushed
        let flushed = sink.flushed.lock().unwrap();
        let total_records: usize = flushed.iter().map(|b| b.len()).sum();
        assert_eq!(total_records, 5, "all 5 records should be flushed");

        // Checkpoint should be advanced
        let cp = checkpoint_store
            .load()
            .unwrap()
            .expect("checkpoint should exist");
        assert_eq!(cp.last_flushed_offset, 4); // offsets 0..4

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_flusher_idempotent_on_re_run() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

        for _ in 0..3 {
            let h = wal.append(make_record(SignalType::Metric)).await.unwrap();
            h.durable().await.unwrap();
        }

        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        // First flush pass
        let flusher = WalFlusher::new(wal.clone(), sink.clone(), checkpoint_store.clone(), 50);
        let flusher_cancel = CancellationToken::new();
        let fc = flusher_cancel.clone();
        let h = tokio::spawn(flusher.run(fc));
        tokio::time::sleep(Duration::from_millis(200)).await;
        flusher_cancel.cancel();
        h.await.unwrap();

        // Second flush pass (should be a no-op)
        let sink2 = Arc::new(MockSink::default());
        let flusher2 = WalFlusher::new(wal.clone(), sink2.clone(), checkpoint_store.clone(), 50);
        let flusher_cancel2 = CancellationToken::new();
        let fc2 = flusher_cancel2.clone();
        let h2 = tokio::spawn(flusher2.run(fc2));
        tokio::time::sleep(Duration::from_millis(200)).await;
        flusher_cancel2.cancel();
        h2.await.unwrap();

        let flushed2 = sink2.flushed.lock().unwrap();
        let total: usize = flushed2.iter().map(|b| b.len()).sum();
        assert_eq!(total, 0, "re-flushing same range should be no-op");

        cancel.cancel();
    }
}
