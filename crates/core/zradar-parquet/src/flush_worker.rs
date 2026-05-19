//! M07-04: Background flush worker for the write buffer.
//!
//! `FlushWorker` drains [`WriteBuffer`](crate::write_buffer::WriteBuffer)
//! slots on two triggers:
//!
//! 1. **Timer** — every `flush_interval_secs` seconds, drain all TTL-expired
//!    slots (those older than the interval).
//! 2. **Notify** — immediately when the buffer signals that a slot exceeded
//!    the size threshold.
//!
//! On cancellation (`CancellationToken::cancelled()`), the worker performs a
//! final `drain_all()` flush so no data is lost on graceful shutdown.
//!
//! ## Error handling
//!
//! Individual flush errors (e.g., disk full) are logged at `error!` level but
//! do not stop the worker.  The slot's data is dropped if the write fails,
//! preventing unbounded memory growth.

use std::sync::Arc;

use anyhow::Context;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::write_buffer::{BufferKey, SignalBatch, WriteBuffer};
use crate::writer::ParquetFileWriter;

/// Background worker that drains `WriteBuffer` slots to Parquet.
pub struct FlushWorker {
    buffer: Arc<WriteBuffer>,
    writer: Arc<ParquetFileWriter>,
    flush_interval_secs: u64,
}

impl FlushWorker {
    /// Create a new `FlushWorker`.
    ///
    /// * `buffer` — shared write buffer populated by the OTLP handlers.
    /// * `writer` — Parquet file writer; the same instance used by the
    ///   direct-write path.
    /// * `flush_interval_secs` — timer tick period and TTL for eligible slots.
    pub fn new(
        buffer: Arc<WriteBuffer>,
        writer: Arc<ParquetFileWriter>,
        flush_interval_secs: u64,
    ) -> Self {
        Self {
            buffer,
            writer,
            flush_interval_secs,
        }
    }

    /// Run the flush loop until `cancel` is triggered.
    ///
    /// This method is intended to be spawned with `tokio::spawn`.
    pub async fn run(self, cancel: CancellationToken) {
        let mut tick = interval(Duration::from_secs(self.flush_interval_secs));
        let notify = self.buffer.flush_notify();

        info!(
            flush_interval_secs = self.flush_interval_secs,
            "FlushWorker started"
        );

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    debug!("FlushWorker: timer tick — draining eligible slots");
                    self.flush_eligible().await;
                }
                _ = notify.notified() => {
                    debug!("FlushWorker: size threshold reached — draining eligible slots");
                    self.flush_eligible().await;
                }
                _ = cancel.cancelled() => {
                    info!("FlushWorker: shutdown signal received — flushing all remaining slots");
                    self.flush_slots(self.buffer.drain_all()).await;
                    info!("FlushWorker: shutdown flush complete");
                    return;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    async fn flush_eligible(&self) {
        let slots = self.buffer.drain_eligible(self.flush_interval_secs);
        self.flush_slots(slots).await;
    }

    async fn flush_slots(&self, slots: Vec<(BufferKey, crate::write_buffer::BufferSlot)>) {
        if slots.is_empty() {
            return;
        }

        debug!(count = slots.len(), "FlushWorker: flushing slots");

        for (key, slot) in slots {
            if let Err(e) = self.flush_one(&key, slot.data).await {
                error!(
                    tenant_id = %key.tenant_id,
                    project_id = %key.project_id,
                    signal_type = %key.signal_type,
                    stream_name = %key.stream_name,
                    error = %e,
                    "FlushWorker: failed to flush slot — data dropped"
                );
            }
        }
    }

    async fn flush_one(&self, key: &BufferKey, data: SignalBatch) -> anyhow::Result<()> {
        match data {
            SignalBatch::Spans(spans) if !spans.is_empty() => {
                self.writer
                    .write_spans(&key.tenant_id, &key.project_id, &key.stream_name, &spans)
                    .await
                    .with_context(|| {
                        format!(
                            "FlushWorker write_spans failed for {}/{}",
                            key.tenant_id, key.stream_name
                        )
                    })?;
            }
            SignalBatch::Metrics(metrics) if !metrics.is_empty() => {
                self.writer
                    .write_metrics(&key.tenant_id, &key.project_id, &key.stream_name, &metrics)
                    .await
                    .with_context(|| {
                        format!(
                            "FlushWorker write_metrics failed for {}/{}",
                            key.tenant_id, key.stream_name
                        )
                    })?;
            }
            SignalBatch::Logs(logs) if !logs.is_empty() => {
                self.writer
                    .write_logs(&key.tenant_id, &key.project_id, &key.stream_name, &logs)
                    .await
                    .with_context(|| {
                        format!(
                            "FlushWorker write_logs failed for {}/{}",
                            key.tenant_id, key.stream_name
                        )
                    })?;
            }
            // Empty batches are no-ops (e.g. size 0 signals that should not
            // have been drained — this is a defensive guard).
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, Span, StreamStats, StreamStatsUpdate,
    };
    use zradar_traits::FileListRepository;

    use crate::write_buffer::{BufferKey, WriteBuffer};
    use crate::writer::{ParquetFileWriter, WriterConfig};

    // ---- minimal stub repo ----

    #[derive(Default)]
    struct CapturingRepo {
        count: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl FileListRepository for CapturingRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            *self.count.lock().unwrap() += 1;
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

    fn make_span(tenant: &str, project: &str) -> Span {
        Span {
            trace_id: Uuid::new_v4().to_string(),
            span_id: Uuid::new_v4().to_string(),
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            service_name: "svc".to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            duration_ns: 1_000_000,
            ..Span::default()
        }
    }

    /// Verify that the flush worker writes buffered spans to Parquet on shutdown.
    #[tokio::test]
    async fn test_flush_worker_drains_on_cancel() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let config = WriterConfig {
            bloom_filter_columns: vec![],
            fsync_before_rename: false,
        };
        let writer = Arc::new(ParquetFileWriter::with_config(
            dir.path().to_path_buf(),
            repo.clone() as Arc<dyn FileListRepository>,
            config,
        ));

        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let tenant = Uuid::new_v4().to_string();
        let project = Uuid::new_v4().to_string();

        let key = BufferKey {
            tenant_id: tenant.clone(),
            project_id: project.clone(),
            signal_type: "traces".to_string(),
            stream_name: "svc".to_string(),
            hour: "2024/01/15/14".to_string(),
        };

        // Push 5 spans — they should be flushed as a single Parquet file on shutdown.
        let spans: Vec<Span> = (0..5).map(|_| make_span(&tenant, &project)).collect();
        buffer.push_spans(key, &spans);
        assert_eq!(buffer.len(), 1, "one slot after push");

        let cancel = CancellationToken::new();
        let worker = FlushWorker::new(buffer.clone(), writer.clone(), 3600);

        // Spawn then immediately cancel.
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move { worker.run(cancel_clone).await });

        cancel.cancel();
        handle.await.unwrap();

        // The buffer should be empty after shutdown flush.
        assert!(
            buffer.is_empty(),
            "buffer must be empty after shutdown flush"
        );

        // Exactly one Parquet file registered (5 spans → 1 file).
        let files_written = *repo.count.lock().unwrap();
        assert_eq!(
            files_written, 1,
            "5 buffered spans should produce 1 Parquet file"
        );
    }

    /// Verify that the flush worker correctly handles an empty buffer on shutdown.
    #[tokio::test]
    async fn test_flush_worker_empty_shutdown() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let writer = Arc::new(ParquetFileWriter::new(
            dir.path().to_path_buf(),
            repo.clone() as Arc<dyn FileListRepository>,
        ));
        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let cancel = CancellationToken::new();
        let worker = FlushWorker::new(buffer, writer, 30);

        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move { worker.run(cancel_clone).await });

        cancel.cancel();
        handle.await.unwrap();

        assert_eq!(*repo.count.lock().unwrap(), 0);
    }
}
