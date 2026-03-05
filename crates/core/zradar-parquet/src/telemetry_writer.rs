//! `TelemetryWriter` implementation backed by Parquet files.

use std::sync::Arc;

use async_trait::async_trait;
use zradar_models::{LogRecord, Metric, Span};
use zradar_traits::TelemetryWriter;

use crate::writer::ParquetFileWriter;

/// Implements `TelemetryWriter` by delegating span writes to `ParquetFileWriter`.
///
/// `insert_metrics` is a no-op stub; metric Parquet support is added in Phase 03.
pub struct ParquetTelemetryWriter {
    writer: Arc<ParquetFileWriter>,
}

impl ParquetTelemetryWriter {
    /// Create a new writer wrapping the given `ParquetFileWriter`.
    pub fn new(writer: Arc<ParquetFileWriter>) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl TelemetryWriter for ParquetTelemetryWriter {
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let first = &spans[0];

        // Derive stream name from service_name; fall back to "default".
        let stream_name = if first.service_name.is_empty() {
            "default".to_string()
        } else {
            first.service_name.clone()
        };

        self.writer
            .write_spans(&first.tenant_id, &first.project_id, &stream_name, spans)
            .await?;

        Ok(())
    }

    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let first = &metrics[0];
        let stream_name = if first.service_name.is_empty() {
            "default".to_string()
        } else {
            first.service_name.clone()
        };

        self.writer
            .write_metrics(&first.tenant_id, &first.project_id, &stream_name, metrics)
            .await?;

        Ok(())
    }

    async fn insert_logs(&self, logs: &[LogRecord]) -> anyhow::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let first = &logs[0];
        let stream_name = if first.service_name.is_empty() {
            "default".to_string()
        } else {
            first.service_name.clone()
        };

        self.writer
            .write_logs(&first.tenant_id, &first.project_id, &stream_name, logs)
            .await?;

        Ok(())
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
}
