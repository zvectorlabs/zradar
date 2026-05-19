//! `TelemetryWriter` implementation backed by Parquet files.
//!
//! `ParquetTelemetryWriter` supports two write modes:
//!
//! * **Direct** (default) — each `insert_*` call immediately writes a Parquet
//!   file via `ParquetFileWriter`.
//! * **Buffered** (M07-04) — `insert_*` calls accumulate records in a
//!   `WriteBuffer`; a background `FlushWorker` batches them into fewer files.
//!   Enable by passing a `WriteBuffer` to `ParquetTelemetryWriter::with_buffer`.

use std::sync::Arc;

use async_trait::async_trait;
use zradar_models::{LogRecord, Metric, Span};
use zradar_traits::TelemetryWriter;

use crate::write_buffer::{BufferKey, WriteBuffer};
use crate::writer::{ParquetFileWriter, ts_ns_to_date_path};

/// Implements `TelemetryWriter` backed by `ParquetFileWriter`.
///
/// When a `WriteBuffer` is present, data is buffered and flushed in batches
/// by the `FlushWorker`.  Without a buffer, each call writes directly.
pub struct ParquetTelemetryWriter {
    writer: Arc<ParquetFileWriter>,
    buffer: Option<Arc<WriteBuffer>>,
}

impl ParquetTelemetryWriter {
    /// Create a writer using the direct (unbuffered) write path.
    pub fn new(writer: Arc<ParquetFileWriter>) -> Self {
        Self {
            writer,
            buffer: None,
        }
    }

    /// Create a writer that accumulates records in `buffer` before writing.
    ///
    /// The caller must also spawn a `FlushWorker` that drains the same buffer.
    pub fn with_buffer(writer: Arc<ParquetFileWriter>, buffer: Arc<WriteBuffer>) -> Self {
        Self {
            writer,
            buffer: Some(buffer),
        }
    }
}

#[async_trait]
impl TelemetryWriter for ParquetTelemetryWriter {
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let first = &spans[0];
        let stream_name = stream_name_or_default(&first.service_name);

        if let Some(buf) = &self.buffer {
            let hour = ts_ns_to_date_path(first.timestamp);
            buf.push_spans(
                BufferKey {
                    tenant_id: first.tenant_id.clone(),
                    project_id: first.project_id.clone(),
                    signal_type: "traces".to_string(),
                    stream_name,
                    hour,
                },
                spans,
            );
        } else {
            self.writer
                .write_spans(&first.tenant_id, &first.project_id, &stream_name, spans)
                .await?;
        }

        Ok(())
    }

    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let first = &metrics[0];
        let stream_name = stream_name_or_default(&first.service_name);

        if let Some(buf) = &self.buffer {
            let hour = ts_ns_to_date_path(first.timestamp);
            buf.push_metrics(
                BufferKey {
                    tenant_id: first.tenant_id.clone(),
                    project_id: first.project_id.clone(),
                    signal_type: "metrics".to_string(),
                    stream_name,
                    hour,
                },
                metrics,
            );
        } else {
            self.writer
                .write_metrics(&first.tenant_id, &first.project_id, &stream_name, metrics)
                .await?;
        }

        Ok(())
    }

    async fn insert_logs(&self, logs: &[LogRecord]) -> anyhow::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let first = &logs[0];
        let stream_name = stream_name_or_default(&first.service_name);

        if let Some(buf) = &self.buffer {
            let hour = ts_ns_to_date_path(first.timestamp);
            buf.push_logs(
                BufferKey {
                    tenant_id: first.tenant_id.clone(),
                    project_id: first.project_id.clone(),
                    signal_type: "logs".to_string(),
                    stream_name,
                    hour,
                },
                logs,
            );
        } else {
            self.writer
                .write_logs(&first.tenant_id, &first.project_id, &stream_name, logs)
                .await?;
        }

        Ok(())
    }
}

fn stream_name_or_default(service_name: &str) -> String {
    if service_name.is_empty() {
        "default".to_string()
    } else {
        service_name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
    };

    // ---------------------------------------------------------------------------
    // Stub repo that captures the most recent registered entry
    // ---------------------------------------------------------------------------

    #[derive(Default)]
    struct CapturingRepo {
        last_entry: Mutex<Option<NewFileListEntry>>,
    }

    #[async_trait::async_trait]
    impl zradar_traits::FileListRepository for CapturingRepo {
        async fn register_file(&self, entry: NewFileListEntry) -> anyhow::Result<i64> {
            *self.last_entry.lock().unwrap() = Some(entry);
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

    fn make_writer_with_repo(
        dir: &std::path::Path,
        repo: Arc<dyn zradar_traits::FileListRepository>,
    ) -> ParquetTelemetryWriter {
        let fw = Arc::new(ParquetFileWriter::new(dir.to_path_buf(), repo));
        ParquetTelemetryWriter::new(fw)
    }

    // ---------------------------------------------------------------------------
    // Unit tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_insert_spans_empty_is_noop() {
        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo,
        ));
        let writer = ParquetTelemetryWriter::new(fw);
        // Must not error on empty slice.
        writer.insert_spans(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_metrics_always_succeeds() {
        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo,
        ));
        let writer = ParquetTelemetryWriter::new(fw);
        writer.insert_metrics(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_name_derived_from_service_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let writer = make_writer_with_repo(dir.path(), repo.clone());

        let span = Span {
            service_name: "my-agent".to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            project_id: Uuid::new_v4().to_string(),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        let entry = repo.last_entry.lock().unwrap();
        assert_eq!(entry.as_ref().unwrap().stream_name, "my-agent");
    }

    #[tokio::test]
    async fn test_stream_name_defaults_when_service_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let writer = make_writer_with_repo(dir.path(), repo.clone());

        // service_name is empty string (the Span default)
        let span = Span {
            tenant_id: Uuid::new_v4().to_string(),
            project_id: Uuid::new_v4().to_string(),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        let entry = repo.last_entry.lock().unwrap();
        assert_eq!(entry.as_ref().unwrap().stream_name, "default");
    }

    // ---- M07-04: buffered path ----

    #[tokio::test]
    async fn test_buffered_insert_spans_goes_to_buffer_not_file() {
        use crate::write_buffer::WriteBuffer;

        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo.clone(),
        ));
        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let writer = ParquetTelemetryWriter::with_buffer(fw, buffer.clone());

        let span = Span {
            service_name: "my-svc".to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            project_id: Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        // Data must be in the buffer, NOT yet written to Parquet.
        assert_eq!(buffer.len(), 1, "one slot should be in the buffer");
        assert!(
            repo.last_entry.lock().unwrap().is_none(),
            "no Parquet file should be written yet"
        );
    }

    #[tokio::test]
    async fn test_stream_name_helper_empty() {
        assert_eq!(stream_name_or_default(""), "default");
    }

    #[tokio::test]
    async fn test_stream_name_helper_non_empty() {
        assert_eq!(stream_name_or_default("my-svc"), "my-svc");
    }
}
