//! Parquet file writer.
//!
//! `ParquetFileWriter` writes a slice of `Span` rows to a Parquet file on
//! local disk, then registers the file in the `file_list` metadata table.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use tracing::debug;
use uuid::Uuid;
use zradar_models::{FileListFilter, LogRecord, Metric, NewFileListEntry, Span, StreamStatsUpdate};
use zradar_traits::FileListRepository;

use crate::schema::logs::logs_to_record_batch;
use crate::schema::metrics::metrics_to_record_batch;
use crate::schema::spans::spans_to_record_batch;

/// Writes `Span` data as Parquet files to a local data directory and registers
/// each file in the `file_list` metadata table.
pub struct ParquetFileWriter {
    /// Root directory for all Parquet files (e.g. `./data/parquet-files`).
    data_dir: PathBuf,
    file_list_repo: Arc<dyn FileListRepository>,
}

impl ParquetFileWriter {
    /// Create a new writer.
    pub fn new(data_dir: PathBuf, file_list_repo: Arc<dyn FileListRepository>) -> Self {
        Self {
            data_dir,
            file_list_repo,
        }
    }

    /// Write `spans` to a Parquet file and register it in `file_list`.
    ///
    /// File path layout:
    /// ```text
    /// {data_dir}/files/{tenant_id}/traces/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{uuid}.parquet
    /// ```
    ///
    /// Returns the absolute path to the written file.
    pub async fn write_spans(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        spans: &[Span],
    ) -> anyhow::Result<String> {
        if spans.is_empty() {
            return Err(anyhow!("write_spans called with empty slice"));
        }

        let batch =
            spans_to_record_batch(spans).context("Failed to convert spans to RecordBatch")?;

        let min_ts_ns = spans.iter().map(|s| s.timestamp).min().unwrap_or(0);
        let max_ts_ns = spans
            .iter()
            .map(|s| s.timestamp.saturating_add(s.duration_ns))
            .max()
            .unwrap_or(0);
        let min_ts_us = min_ts_ns / 1_000;
        let max_ts_us = max_ts_ns / 1_000;

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "traces",
            batch,
            min_ts_ns,
            min_ts_us,
            max_ts_us,
            spans.len() as i64,
        )
        .await
    }

    /// Write `metrics` to a Parquet file and register it in `file_list`.
    ///
    /// File path layout:
    /// ```text
    /// {data_dir}/files/{tenant_id}/metrics/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{uuid}.parquet
    /// ```
    pub async fn write_metrics(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        metrics: &[Metric],
    ) -> anyhow::Result<String> {
        if metrics.is_empty() {
            return Err(anyhow!("write_metrics called with empty slice"));
        }

        let batch =
            metrics_to_record_batch(metrics).context("Failed to convert metrics to RecordBatch")?;

        let min_ts_ns = metrics.iter().map(|m| m.timestamp).min().unwrap_or(0);
        let max_ts_ns = metrics.iter().map(|m| m.timestamp).max().unwrap_or(0);
        let min_ts_us = min_ts_ns / 1_000;
        let max_ts_us = max_ts_ns / 1_000;

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "metrics",
            batch,
            min_ts_ns,
            min_ts_us,
            max_ts_us,
            metrics.len() as i64,
        )
        .await
    }

    /// Write `logs` to a Parquet file and register it in `file_list`.
    ///
    /// File path layout:
    /// ```text
    /// {data_dir}/files/{tenant_id}/logs/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{uuid}.parquet
    /// ```
    pub async fn write_logs(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        logs: &[LogRecord],
    ) -> anyhow::Result<String> {
        if logs.is_empty() {
            return Err(anyhow!("write_logs called with empty slice"));
        }

        let batch =
            logs_to_record_batch(logs).context("Failed to convert logs to RecordBatch")?;

        let min_ts_ns = logs.iter().map(|l| l.timestamp).min().unwrap_or(0);
        let max_ts_ns = logs.iter().map(|l| l.timestamp).max().unwrap_or(0);
        let min_ts_us = min_ts_ns / 1_000;
        let max_ts_us = max_ts_ns / 1_000;

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "logs",
            batch,
            min_ts_ns,
            min_ts_us,
            max_ts_us,
            logs.len() as i64,
        )
        .await
    }

    /// Query which files have already been written for a given filter.
    ///
    /// Exposed so tests and tooling can inspect the registry.
    pub async fn query_files(
        &self,
        filter: FileListFilter,
    ) -> anyhow::Result<Vec<zradar_models::FileListEntry>> {
        self.file_list_repo.query_files(filter).await
    }

    /// Internal helper: write a `RecordBatch` to a Parquet file and register it.
    #[allow(clippy::too_many_arguments)]
    async fn write_batch(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        signal_type: &str,
        batch: arrow::record_batch::RecordBatch,
        min_ts_ns: i64,
        min_ts_us: i64,
        max_ts_us: i64,
        record_count: i64,
    ) -> anyhow::Result<String> {
        let date_path = ts_ns_to_date_path(min_ts_ns);
        let file_id = Uuid::new_v4();
        let relative = format!(
            "files/{}/{}/{}/{}/{}.parquet",
            tenant_id, signal_type, stream_name, date_path, file_id
        );
        let full_path = self.data_dir.join(&relative);

        tokio::fs::create_dir_all(
            full_path
                .parent()
                .ok_or_else(|| anyhow!("invalid file path: {}", full_path.display()))?,
        )
        .await
        .context("Failed to create Parquet directory")?;

        let schema = batch.schema();
        let batch_clone = batch.clone();
        let path_clone = full_path.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            use parquet::arrow::ArrowWriter;
            use parquet::basic::Compression;
            use parquet::file::properties::WriterProperties;

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build();

            let file = std::fs::File::create(&path_clone)
                .with_context(|| format!("Failed to create file: {}", path_clone.display()))?;
            let mut writer = ArrowWriter::try_new(file, schema, Some(props))
                .context("Failed to create ArrowWriter")?;
            writer
                .write(&batch_clone)
                .context("Failed to write RecordBatch")?;
            writer.close().context("Failed to close ArrowWriter")?;
            Ok(())
        })
        .await
        .context("spawn_blocking panicked")?
        .context("Parquet write failed")?;

        let metadata = tokio::fs::metadata(&full_path)
            .await
            .context("Failed to stat Parquet file")?;
        let compressed_size = metadata.len() as i64;
        let original_size = batch.get_array_memory_size() as i64;

        let full_path_str = full_path.to_string_lossy().into_owned();
        let now_us = chrono::Utc::now().timestamp_micros();
        let tenant_uuid = parse_uuid_or_nil(tenant_id);
        let project_uuid = parse_uuid_or_nil(project_id);

        let entry = NewFileListEntry {
            tenant_id: tenant_uuid,
            project_id: project_uuid,
            signal_type: signal_type.to_string(),
            stream_name: stream_name.to_string(),
            date: date_path.clone(),
            file_path: full_path_str.clone(),
            location: "local".to_string(),
            min_ts: min_ts_us,
            max_ts: max_ts_us,
            records: record_count,
            original_size,
            compressed_size,
            created_at: now_us,
            updated_at: now_us,
        };
        self.file_list_repo
            .register_file(entry)
            .await
            .context("Failed to register file in file_list")?;

        let stats = StreamStatsUpdate {
            tenant_id: tenant_uuid,
            project_id: project_uuid,
            signal_type: signal_type.to_string(),
            stream_name: stream_name.to_string(),
            min_ts: min_ts_us,
            max_ts: max_ts_us,
            records_delta: record_count,
            original_size_delta: original_size,
            compressed_size_delta: compressed_size,
        };
        self.file_list_repo
            .upsert_stream_stats(stats)
            .await
            .context("Failed to upsert stream_stats")?;

        debug!(
            path = %full_path_str,
            signal_type,
            records = record_count,
            compressed_bytes = compressed_size,
            "Parquet file written"
        );

        Ok(full_path_str)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a nanosecond UNIX timestamp as `YYYY/MM/DD/HH`.
fn ts_ns_to_date_path(ts_ns: i64) -> String {
    let secs = ts_ns / 1_000_000_000;
    let nanos = ts_ns.rem_euclid(1_000_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
    dt.format("%Y/%m/%d/%H").to_string()
}

/// Parse a UUID string, returning `Uuid::nil()` on failure.
fn parse_uuid_or_nil(s: &str) -> Uuid {
    Uuid::parse_str(s).unwrap_or(Uuid::nil())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ts_ns_to_date_path() {
        // 2024-01-15 14:30:00 UTC in nanoseconds
        let ts_ns = 1_705_329_000_000_000_000_i64;
        let path = ts_ns_to_date_path(ts_ns);
        assert_eq!(path, "2024/01/15/14");
    }

    #[test]
    fn test_parse_uuid_or_nil_valid() {
        let id = Uuid::new_v4();
        assert_eq!(parse_uuid_or_nil(&id.to_string()), id);
    }

    #[test]
    fn test_parse_uuid_or_nil_invalid() {
        assert_eq!(parse_uuid_or_nil("not-a-uuid"), Uuid::nil());
    }
}
