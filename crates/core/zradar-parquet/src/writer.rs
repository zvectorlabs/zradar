//! Parquet file writer.
//!
//! `ParquetFileWriter` writes telemetry data to Parquet files on local disk
//! and registers each file in the `file_list` metadata table.
//!
//! ## Write safety (M07-03)
//!
//! Files are written atomically:
//! 1. Write bytes to `<uuid>.par` (temp extension).
//! 2. Optional fsync on the temp file before rename (controlled by `WriterConfig`).
//! 3. `std::fs::rename(.par → .parquet)` — POSIX-atomic: readers never see a
//!    partial file because the rename is either fully visible or not visible.
//!
//! On startup call `recovery::recover_incomplete_writes` to delete any orphaned
//! `.par` files left by a crash between write and rename.
//!
//! ## Bloom filters (M07-02)
//!
//! `WriterConfig::bloom_filter_columns` controls which columns get a Parquet
//! bloom filter.  The defaults (`trace_id`, `span_id`, `id`) allow DataFusion
//! to skip entire row groups when doing point lookups on those columns.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tracing::debug;
use uuid::Uuid;
use zradar_models::{FileListFilter, LogRecord, Metric, NewFileListEntry, Span, StreamStatsUpdate};
use zradar_policy::{DecisionSummary, SignalKind, UsageTracker, WriteSample};
use zradar_traits::FileListRepository;

use crate::schema::logs::logs_to_record_batch;
use crate::schema::metrics::metrics_to_record_batch;
use crate::schema::spans::spans_to_record_batch;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for `ParquetFileWriter` (bloom filters, fsync behaviour).
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Parquet column names that should have a bloom filter built at write time.
    /// Enables DataFusion to skip row groups on point lookups.
    pub bloom_filter_columns: Vec<String>,
    /// When `true`, fsync the temp `.par` file before the atomic rename.
    /// Set to `false` in tests to avoid hitting actual disk.
    pub fsync_before_rename: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            bloom_filter_columns: vec!["trace_id".into(), "span_id".into(), "id".into()],
            fsync_before_rename: true,
        }
    }
}

impl WriterConfig {
    /// Create from `ParquetStorageConfig` fields.
    pub fn from_storage_config(
        bloom_filter_columns: Vec<String>,
        fsync_before_rename: bool,
    ) -> Self {
        Self {
            bloom_filter_columns,
            fsync_before_rename,
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writes telemetry data as Parquet files to local disk and registers each file
/// in the `file_list` metadata table.
pub struct ParquetFileWriter {
    /// Root directory for all Parquet files (e.g. `./data/parquet-files`).
    data_dir: PathBuf,
    file_list_repo: Arc<dyn FileListRepository>,
    usage_tracker: Option<Arc<dyn UsageTracker>>,
    config: WriterConfig,
}

impl ParquetFileWriter {
    /// Create a new writer with default `WriterConfig`.
    pub fn new(data_dir: PathBuf, file_list_repo: Arc<dyn FileListRepository>) -> Self {
        Self {
            data_dir: make_absolute(data_dir),
            file_list_repo,
            usage_tracker: None,
            config: WriterConfig::default(),
        }
    }

    /// Create a writer with explicit `WriterConfig`.
    pub fn with_config(
        data_dir: PathBuf,
        file_list_repo: Arc<dyn FileListRepository>,
        config: WriterConfig,
    ) -> Self {
        Self {
            data_dir: make_absolute(data_dir),
            file_list_repo,
            usage_tracker: None,
            config,
        }
    }

    pub fn with_config_and_usage_tracker(
        data_dir: PathBuf,
        file_list_repo: Arc<dyn FileListRepository>,
        config: WriterConfig,
        usage_tracker: Arc<dyn UsageTracker>,
    ) -> Self {
        Self {
            data_dir: make_absolute(data_dir),
            file_list_repo,
            usage_tracker: Some(usage_tracker),
            config,
        }
    }

    // -----------------------------------------------------------------------
    // Public signal-type entry points
    // -----------------------------------------------------------------------

    /// Write `spans` to a Parquet file and register it in `file_list`.
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

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "traces",
            batch,
            min_ts_ns,
            min_ts_ns / 1_000,
            max_ts_ns / 1_000,
            spans.len() as i64,
        )
        .await
    }

    /// Write `metrics` to a Parquet file and register it in `file_list`.
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

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "metrics",
            batch,
            min_ts_ns,
            min_ts_ns / 1_000,
            max_ts_ns / 1_000,
            metrics.len() as i64,
        )
        .await
    }

    /// Write `logs` to a Parquet file and register it in `file_list`.
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

        let batch = logs_to_record_batch(logs).context("Failed to convert logs to RecordBatch")?;

        let min_ts_ns = logs.iter().map(|l| l.timestamp).min().unwrap_or(0);
        let max_ts_ns = logs.iter().map(|l| l.timestamp).max().unwrap_or(0);

        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            "logs",
            batch,
            min_ts_ns,
            min_ts_ns / 1_000,
            max_ts_ns / 1_000,
            logs.len() as i64,
        )
        .await
    }

    /// Write a pre-converted `RecordBatch` directly (used by `FlushWorker`).
    ///
    /// Timestamps must be in **nanoseconds** (`min_ts_ns`, `max_ts_ns`).
    #[allow(clippy::too_many_arguments)]
    pub async fn write_record_batch(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        signal_type: &str,
        batch: RecordBatch,
        min_ts_ns: i64,
        max_ts_ns: i64,
        record_count: i64,
    ) -> anyhow::Result<String> {
        self.write_batch(
            tenant_id,
            project_id,
            stream_name,
            signal_type,
            batch,
            min_ts_ns,
            min_ts_ns / 1_000,
            max_ts_ns / 1_000,
            record_count,
        )
        .await
    }

    /// Query which files have already been written for a given filter.
    pub async fn query_files(
        &self,
        filter: FileListFilter,
    ) -> anyhow::Result<Vec<zradar_models::FileListEntry>> {
        self.file_list_repo.query_files(filter).await
    }

    // -----------------------------------------------------------------------
    // Core write implementation
    // -----------------------------------------------------------------------

    /// Write a `RecordBatch` to Parquet and register in `file_list`.
    ///
    /// Uses the atomic write sequence:
    ///   1. Write bytes to `<uuid>.par` (temp).
    ///   2. Fsync (if `config.fsync_before_rename`).
    ///   3. Rename `.par` → `.parquet` (POSIX-atomic).
    #[allow(clippy::too_many_arguments)]
    async fn write_batch(
        &self,
        tenant_id: &str,
        project_id: &str,
        stream_name: &str,
        signal_type: &str,
        batch: RecordBatch,
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
        let bloom_columns = self.config.bloom_filter_columns.clone();
        let fsync = self.config.fsync_before_rename;

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            use parquet::arrow::ArrowWriter;
            use parquet::schema::types::ColumnPath;

            // --- Build WriterProperties with bloom filters (M07-02) ---
            let mut props_builder = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_statistics_enabled(EnabledStatistics::Page);

            for col in &bloom_columns {
                props_builder = props_builder
                    .set_column_bloom_filter_enabled(ColumnPath::from(col.as_str()), true);
            }
            let props = props_builder.build();

            // --- Step 1: write to temp .par file (M07-03) ---
            let temp_path = path_clone.with_extension("par");
            {
                let file = std::fs::File::create(&temp_path).with_context(|| {
                    format!("Failed to create temp file: {}", temp_path.display())
                })?;
                let mut writer = ArrowWriter::try_new(file, schema, Some(props))
                    .context("Failed to create ArrowWriter")?;
                writer
                    .write(&batch_clone)
                    .context("Failed to write RecordBatch")?;
                writer.close().context("Failed to close ArrowWriter")?;
                // File handle dropped here — ArrowWriter flushes on close.
            }

            // --- Step 2: fsync temp file before rename (M07-03) ---
            if fsync {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&temp_path)
                    .with_context(|| {
                        format!(
                            "Failed to reopen temp file for fsync: {}",
                            temp_path.display()
                        )
                    })?;
                file.sync_all().with_context(|| {
                    format!("Failed to fsync temp file: {}", temp_path.display())
                })?;
            }

            // --- Step 3: atomic rename .par → .parquet (M07-03) ---
            std::fs::rename(&temp_path, &path_clone).with_context(|| {
                format!(
                    "Failed to rename {} → {}",
                    temp_path.display(),
                    path_clone.display()
                )
            })?;

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

        let registered_file_id = self
            .file_list_repo
            .register_file(NewFileListEntry {
                tenant_id: tenant_uuid,
                project_id: project_uuid,
                signal_type: signal_type.to_string(),
                stream_name: stream_name.to_string(),
                date: date_path,
                file_path: full_path_str.clone(),
                location: "local".to_string(),
                min_ts: min_ts_us,
                max_ts: max_ts_us,
                records: record_count,
                original_size,
                compressed_size,
                created_at: now_us,
                updated_at: now_us,
                wal_replay_offset: None,
            })
            .await
            .context("Failed to register file in file_list")?;

        if let Some(usage_tracker) = &self.usage_tracker {
            usage_tracker
                .record_write(WriteSample {
                    tenant_id: tenant_uuid,
                    project_id: project_uuid,
                    signal: signal_kind(signal_type),
                    stream_name: Some(stream_name.to_string()),
                    compressed_bytes: compressed_size,
                    original_bytes: Some(original_size),
                    records: record_count,
                    file_id: Some(registered_file_id),
                    decision: DecisionSummary::Allow,
                    flushed_at: now_us,
                })
                .await;
        }

        self.file_list_repo
            .upsert_stream_stats(StreamStatsUpdate {
                tenant_id: tenant_uuid,
                project_id: project_uuid,
                signal_type: signal_type.to_string(),
                stream_name: stream_name.to_string(),
                min_ts: min_ts_us,
                max_ts: max_ts_us,
                records_delta: record_count,
                original_size_delta: original_size,
                compressed_size_delta: compressed_size,
            })
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
pub(crate) fn ts_ns_to_date_path(ts_ns: i64) -> String {
    let secs = ts_ns / 1_000_000_000;
    let nanos = ts_ns.rem_euclid(1_000_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
    dt.format("%Y/%m/%d/%H").to_string()
}

/// Parse a UUID string, returning `Uuid::nil()` on failure.
pub(crate) fn parse_uuid_or_nil(s: &str) -> Uuid {
    Uuid::parse_str(s).unwrap_or(Uuid::nil())
}

fn signal_kind(signal_type: &str) -> SignalKind {
    match signal_type {
        "traces" => SignalKind::Traces,
        "logs" => SignalKind::Logs,
        "metrics" => SignalKind::Metrics,
        "rum" => SignalKind::Rum,
        "session_replay" => SignalKind::SessionReplay,
        "error_tracking" => SignalKind::ErrorTracking,
        _ => SignalKind::All,
    }
}

/// Convert a potentially-relative path to an absolute path by joining with
/// the process working directory. Does not require the path to exist yet.
fn make_absolute(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use uuid::Uuid;
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
    };

    // ---- minimal stub repo ----

    #[derive(Default)]
    struct CapturingRepo {
        registered: Mutex<Vec<NewFileListEntry>>,
    }

    #[async_trait::async_trait]
    impl FileListRepository for CapturingRepo {
        async fn register_file(&self, e: NewFileListEntry) -> anyhow::Result<i64> {
            self.registered.lock().unwrap().push(e);
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

    fn make_writer(dir: &TempDir) -> (ParquetFileWriter, Arc<CapturingRepo>) {
        let repo = Arc::new(CapturingRepo::default());
        let config = WriterConfig {
            bloom_filter_columns: vec!["trace_id".into(), "span_id".into()],
            fsync_before_rename: false, // skip fsync in unit tests
        };
        let w = ParquetFileWriter::with_config(dir.path().to_path_buf(), repo.clone(), config);
        (w, repo)
    }

    fn make_span(tenant: &str, project: &str) -> Span {
        Span {
            trace_id: Uuid::new_v4().to_string(),
            span_id: Uuid::new_v4().to_string(),
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            service_name: "test-svc".to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            duration_ns: 1_000_000,
            ..Span::default()
        }
    }

    // ---- M07-02: bloom filter columns are set ----

    #[test]
    fn test_writer_config_default_bloom_columns() {
        let cfg = WriterConfig::default();
        assert!(cfg.bloom_filter_columns.contains(&"trace_id".to_string()));
        assert!(cfg.bloom_filter_columns.contains(&"span_id".to_string()));
        assert!(cfg.bloom_filter_columns.contains(&"id".to_string()));
    }

    // ---- M07-03: atomic write — no .par file left after success ----

    #[tokio::test]
    async fn test_write_spans_produces_parquet_not_par() {
        let dir = TempDir::new().unwrap();
        let (writer, repo) = make_writer(&dir);
        let tenant = Uuid::new_v4().to_string();
        let project = Uuid::new_v4().to_string();
        let span = make_span(&tenant, &project);

        let path = writer
            .write_spans(&tenant, &project, "svc", &[span])
            .await
            .unwrap();

        // .parquet file exists
        assert!(
            std::path::Path::new(&path).exists(),
            "parquet file must exist"
        );
        assert!(path.ends_with(".parquet"), "must end with .parquet");

        // no orphaned .par file
        let par = std::path::Path::new(&path).with_extension("par");
        assert!(
            !par.exists(),
            ".par temp file must not remain after success"
        );

        // registered in file_list
        let reg = repo.registered.lock().unwrap();
        assert_eq!(reg.len(), 1);
        assert_eq!(reg[0].records, 1);
    }

    // ---- round-trip: write then read back via DataFusion ----

    #[tokio::test]
    async fn test_write_spans_round_trip_datafusion() {
        use crate::reader::ParquetFileReader;
        use zradar_models::{FileListEntry, FileListFilter};

        let dir = TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let config = WriterConfig {
            bloom_filter_columns: vec!["trace_id".into()],
            fsync_before_rename: false,
        };
        let writer = Arc::new(ParquetFileWriter::with_config(
            dir.path().to_path_buf(),
            repo.clone(),
            config,
        ));

        let tenant = Uuid::new_v4().to_string();
        let project = Uuid::new_v4().to_string();
        let span = make_span(&tenant, &project);
        let trace_id = span.trace_id.clone();

        let file_path = writer
            .write_spans(&tenant, &project, "svc", &[span])
            .await
            .unwrap();

        // Build a repo stub that returns the written file
        struct SingleFileRepo(String, Uuid, Uuid);
        #[async_trait::async_trait]
        impl FileListRepository for SingleFileRepo {
            async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
                Ok(1)
            }
            async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
                Ok(vec![FileListEntry {
                    id: 1,
                    tenant_id: self.1,
                    project_id: self.2,
                    signal_type: "traces".to_string(),
                    stream_name: "svc".to_string(),
                    date: "2024/01/01/00".to_string(),
                    file_path: self.0.clone(),
                    location: "local".to_string(),
                    min_ts: 0,
                    max_ts: i64::MAX,
                    records: 1,
                    original_size: 0,
                    compressed_size: 0,
                    deleted: false,
                    created_at: 0,
                    updated_at: 0,
                }])
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

        let tenant_uuid = parse_uuid_or_nil(&tenant);
        let project_uuid = parse_uuid_or_nil(&project);
        let reader = ParquetFileReader::new(
            dir.path().to_path_buf(),
            Arc::new(SingleFileRepo(file_path, tenant_uuid, project_uuid)),
        );

        let batches = reader
            .query_parquet(
                FileListFilter::default(),
                &format!("SELECT trace_id FROM spans WHERE trace_id = '{trace_id}'"),
            )
            .await
            .unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "round-trip: span must be queryable after write");
    }

    // ---- helper tests ----

    #[test]
    fn test_ts_ns_to_date_path() {
        let ts_ns = 1_705_329_000_000_000_000_i64; // 2024-01-15 14:30:00 UTC
        assert_eq!(ts_ns_to_date_path(ts_ns), "2024/01/15/14");
    }

    #[test]
    fn test_ts_ns_to_date_path_zero() {
        assert_eq!(ts_ns_to_date_path(0), "1970/01/01/00");
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
