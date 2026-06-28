/// WAL replayer: on startup, re-materializes any records that were acknowledged
/// but not yet flushed to Parquet.
///
/// The replay sequence:
/// 1. Load checkpoint (last flushed offset).
/// 2. Iterate segments from checkpoint forward.
/// 3. For each record past the checkpoint offset:
///    - Check `FileListRepository::already_flushed` for idempotency.
///    - If not already flushed, forward to the flush sink.
/// 4. On torn-write detection, truncate the tail and stop.
/// 5. Save final checkpoint.
use std::sync::Arc;

use tracing::{info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::flusher::FlushSink;
use crate::record::WalRecord;
use crate::segment::{self, SegmentError, SegmentReader};

/// Replays unflushed WAL records during server startup.
pub struct WalReplayer {
    wal_dir: std::path::PathBuf,
    sink: Arc<dyn FlushSink>,
    checkpoint_store: Arc<CheckpointStore>,
    replay_batch_max_bytes: u64,
}

impl WalReplayer {
    /// Create a new replayer.
    pub fn new(
        wal_dir: std::path::PathBuf,
        sink: Arc<dyn FlushSink>,
        checkpoint_store: Arc<CheckpointStore>,
        replay_batch_max_bytes: u64,
    ) -> Self {
        Self {
            wal_dir,
            sink,
            checkpoint_store,
            replay_batch_max_bytes,
        }
    }

    /// Replay all unflushed WAL records. This must complete before the OTLP
    /// listener is bound (ensuring no traffic is accepted until replay finishes).
    ///
    /// Returns the number of records replayed.
    pub async fn replay(&self) -> anyhow::Result<u64> {
        let checkpoint = self.checkpoint_store.load()?;
        let segments = segment::list_segments(&self.wal_dir)?;

        if segments.is_empty() {
            info!("WAL replay: no segments found, nothing to replay");
            return Ok(0);
        }

        let start_segment_id = checkpoint
            .as_ref()
            .map_or(0, |cp| cp.last_flushed_segment_id);
        let already_flushed_offset: Option<u64> =
            checkpoint.as_ref().map(|cp| cp.last_flushed_offset);

        let mut total_replayed: u64 = 0;
        let mut last_good_segment_id: u64 = start_segment_id;
        let mut last_good_offset: u64 = already_flushed_offset.unwrap_or(0);

        for &seg_id in &segments {
            if seg_id < start_segment_id {
                continue;
            }

            let mut reader = match SegmentReader::open(&self.wal_dir, seg_id) {
                Ok(r) => r,
                Err(SegmentError::InvalidMagic | SegmentError::UnsupportedVersion(_)) => {
                    return Err(anyhow::anyhow!(
                        "corrupt segment header in segment {seg_id}, cannot replay"
                    ));
                }
                Err(e) => {
                    warn!(segment_id = seg_id, error = %e, "cannot open segment for replay");
                    break;
                }
            };

            let mut batch: Vec<WalRecord> = Vec::new();
            let mut batch_bytes: u64 = 0;
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

                        let rec_size = rec.payload.len() as u64 + 64; // rough estimate
                        batch_bytes += rec_size;
                        max_offset_in_batch = Some(
                            max_offset_in_batch
                                .map_or(rec.assigned_offset, |m: u64| m.max(rec.assigned_offset)),
                        );
                        batch.push(rec);

                        // Flush in batches to bound memory
                        if batch_bytes >= self.replay_batch_max_bytes {
                            let count = batch.len() as u64;
                            self.sink.flush_records(&batch).await?;
                            total_replayed += count;

                            if let Some(max_off) = max_offset_in_batch {
                                last_good_segment_id = seg_id;
                                last_good_offset = max_off;
                            }

                            batch.clear();
                            batch_bytes = 0;
                            max_offset_in_batch = None;
                        }
                    }
                    Ok(None) => break,
                    Err(SegmentError::Record(_)) => {
                        // Torn write — truncate tail and stop replay
                        let trunc_point = reader.truncation_point();
                        warn!(
                            segment_id = seg_id,
                            truncation_offset = trunc_point,
                            "torn write detected during replay, truncating"
                        );
                        segment::truncate_segment(&self.wal_dir, seg_id, trunc_point)?;
                        break;
                    }
                    Err(e) => {
                        warn!(segment_id = seg_id, error = %e, "error during replay");
                        break;
                    }
                }
            }

            // Flush remaining batch for this segment
            if !batch.is_empty() {
                let count = batch.len() as u64;
                self.sink.flush_records(&batch).await?;
                total_replayed += count;

                if let Some(max_off) = max_offset_in_batch {
                    last_good_segment_id = seg_id;
                    last_good_offset = max_off;
                }
            }
        }

        // Save checkpoint after replay
        if total_replayed > 0 {
            self.checkpoint_store.save(&Checkpoint {
                last_flushed_segment_id: last_good_segment_id,
                last_flushed_offset: last_good_offset,
                wal_format_version: 1,
            })?;
        }

        info!(records_replayed = total_replayed, "WAL replay complete");

        Ok(total_replayed)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use crate::Wal;
    use crate::config::WalConfig;
    use crate::record::SignalType;
    use bytes::Bytes;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

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

    fn make_record() -> WalRecord {
        WalRecord {
            signal_type: SignalType::Trace,
            workspace_id: WorkspaceId::new(),
            arrival_timestamp_ns: 1_700_000_000_000_000_000,
            assigned_offset: 0,
            payload: Bytes::from(vec![0xEE; 128]),
        }
    }

    #[tokio::test]
    async fn test_replay_recovers_unflushed_records() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        // Write 10 records to WAL
        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());
        for _ in 0..10 {
            let h = wal.append(make_record()).await.unwrap();
            h.durable().await.unwrap();
        }
        cancel.cancel();
        drop(wal);

        // No checkpoint exists → all 10 should be replayed
        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        let replayer = WalReplayer::new(
            tmp.path().to_path_buf(),
            sink.clone(),
            checkpoint_store.clone(),
            64 * 1024 * 1024,
        );

        let replayed = replayer.replay().await.unwrap();
        assert_eq!(replayed, 10);

        let flushed = sink.flushed.lock().unwrap();
        let total: usize = flushed.iter().map(|b| b.len()).sum();
        assert_eq!(total, 10);
    }

    #[tokio::test]
    async fn test_replay_skips_already_flushed() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());
        for _ in 0..10 {
            let h = wal.append(make_record()).await.unwrap();
            h.durable().await.unwrap();
        }
        cancel.cancel();
        drop(wal);

        // Save checkpoint indicating offsets 0..4 are flushed
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));
        checkpoint_store
            .save(&Checkpoint {
                last_flushed_segment_id: 0,
                last_flushed_offset: 4,
                wal_format_version: 1,
            })
            .unwrap();

        let sink = Arc::new(MockSink::default());
        let replayer = WalReplayer::new(
            tmp.path().to_path_buf(),
            sink.clone(),
            checkpoint_store,
            64 * 1024 * 1024,
        );

        let replayed = replayer.replay().await.unwrap();
        assert_eq!(replayed, 5); // offsets 5..9

        let flushed = sink.flushed.lock().unwrap();
        let total: usize = flushed.iter().map(|b| b.len()).sum();
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn test_replay_handles_torn_write() {
        let tmp = TempDir::new().unwrap();
        let cancel = CancellationToken::new();
        let config = WalConfig {
            segment_max_bytes: 10 * 1024 * 1024,
            group_commit_window_ms: 1,
            ..Default::default()
        };

        let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());
        for _ in 0..5 {
            let h = wal.append(make_record()).await.unwrap();
            h.durable().await.unwrap();
        }
        cancel.cancel();
        drop(wal);

        // Append garbage to simulate torn write
        let seg_path = segment::segment_path(tmp.path(), 0);
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        f.write_all(&[0xFF; 20]).unwrap();

        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        let replayer = WalReplayer::new(
            tmp.path().to_path_buf(),
            sink.clone(),
            checkpoint_store,
            64 * 1024 * 1024,
        );

        let replayed = replayer.replay().await.unwrap();
        assert_eq!(replayed, 5);

        // Segment should have been truncated — re-reading should be clean
        let mut reader = SegmentReader::open(tmp.path(), 0).unwrap();
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_replay_empty_wal() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path()).unwrap();

        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        let replayer = WalReplayer::new(
            tmp.path().to_path_buf(),
            sink.clone(),
            checkpoint_store,
            64 * 1024 * 1024,
        );

        let replayed = replayer.replay().await.unwrap();
        assert_eq!(replayed, 0);
    }

    #[tokio::test]
    async fn test_replay_corrupt_header_returns_error() {
        let tmp = TempDir::new().unwrap();
        let seg_path = segment::segment_path(tmp.path(), 0);
        // Write a file with bad magic
        std::fs::write(&seg_path, b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x00").unwrap();

        let sink = Arc::new(MockSink::default());
        let checkpoint_store = Arc::new(CheckpointStore::new(tmp.path()));

        let replayer = WalReplayer::new(
            tmp.path().to_path_buf(),
            sink,
            checkpoint_store,
            64 * 1024 * 1024,
        );

        let result = replayer.replay().await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("corrupt segment header")
        );
    }
}
