//! Parquet file reader backed by DataFusion.
//!
//! `ParquetFileReader` accepts a `FileListFilter` (for coarse file pruning via
//! the `file_list` metadata table) and a SQL string (for fine-grained row
//! filtering and aggregation). DataFusion executes the SQL against the Parquet
//! files and returns `RecordBatch` results with automatic predicate pushdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use zradar_models::FileListFilter;
use zradar_traits::FileListRepository;

/// Queries Parquet files stored on local disk (Phase 01/02).
///
/// Phase 04 will add S3 support via the existing `BlockStorage` trait.
pub struct ParquetFileReader {
    /// Root directory where Parquet files are stored.
    _data_dir: PathBuf,
    file_list_repo: Arc<dyn FileListRepository>,
}

impl ParquetFileReader {
    /// Create a new reader.
    pub fn new(data_dir: PathBuf, file_list_repo: Arc<dyn FileListRepository>) -> Self {
        Self {
            _data_dir: data_dir,
            file_list_repo,
        }
    }

    /// Execute `sql` against the Parquet files that match `filter`.
    ///
    /// The SQL must reference a table called `spans`.  Internally the reader
    /// registers each matching file as `spans_0`, `spans_1`, … and creates a
    /// `spans` view that unions them all, then executes the provided SQL.
    ///
    /// Returns an empty `Vec` when no files match the filter.
    pub async fn query_parquet(
        &self,
        filter: FileListFilter,
        sql: &str,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        self.query_parquet_as(filter, "spans", sql).await
    }

    /// Execute `sql` against the Parquet files that match `filter`, using
    /// `table_name` as the SQL table name in the DataFusion context.
    ///
    /// This generalises `query_parquet` so callers can query "metrics" or
    /// "logs" files with the same infrastructure.
    ///
    /// Returns an empty `Vec` when no files match the filter.
    pub async fn query_parquet_as(
        &self,
        filter: FileListFilter,
        table_name: &str,
        sql: &str,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let files = self
            .file_list_repo
            .query_files(filter)
            .await
            .context("Failed to query file_list")?;

        if files.is_empty() {
            return Ok(vec![]);
        }

        let mut config = SessionConfig::new();
        // DataFusion 44 defaults to Utf8View (StringViewArray) for Parquet string columns.
        // Disable this so string columns come back as StringArray (Utf8), which our
        // record_batch_to_* converters expect.
        config.options_mut().execution.parquet.schema_force_view_types = false;
        let ctx = SessionContext::new_with_config(config);

        // Register every Parquet file with a unique alias.
        for (i, file) in files.iter().enumerate() {
            ctx.register_parquet(
                &format!("{table_name}_{i}"),
                &file.file_path,
                ParquetReadOptions::default(),
            )
            .await
            .with_context(|| format!("Failed to register parquet file: {}", file.file_path))?;
        }

        // Create a view that UNION ALLs all registered files.
        let parts: Vec<String> = (0..files.len())
            .map(|i| format!("SELECT * FROM {table_name}_{i}"))
            .collect();
        let view_sql = format!("CREATE VIEW {table_name} AS {}", parts.join(" UNION ALL "));
        ctx.sql(&view_sql)
            .await
            .context("Failed to create view")?
            .collect()
            .await
            .context("Failed to execute CREATE VIEW")?;

        // Execute the caller-provided SQL.
        let df = ctx.sql(sql).await.context("Failed to parse query SQL")?;
        df.collect()
            .await
            .context("Failed to collect query results")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
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

    #[tokio::test]
    async fn test_query_parquet_returns_empty_when_no_files() {
        let reader = ParquetFileReader::new(PathBuf::from("/tmp"), Arc::new(EmptyRepo));
        let result = reader
            .query_parquet(FileListFilter::default(), "SELECT * FROM spans")
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    /// Full round-trip: write a Parquet file, read it back via DataFusion.
    #[tokio::test]
    async fn test_round_trip_write_then_query() {
        use crate::schema::spans::spans_to_record_batch;
        use parquet::arrow::ArrowWriter;
        use tempfile::TempDir;
        use zradar_models::Span;

        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.parquet");

        // Write two spans to a Parquet file directly.
        let s1 = Span {
            trace_id: "trace-001".to_string(),
            project_id: "proj-001".to_string(),
            status_code: "OK".to_string(),
            ..Span::default()
        };
        let s2 = Span {
            trace_id: "trace-002".to_string(),
            project_id: "proj-001".to_string(),
            status_code: "ERROR".to_string(),
            ..Span::default()
        };

        let batch = spans_to_record_batch(&[s1, s2]).unwrap();
        let schema = batch.schema();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Build a repo that returns this single file.
        let entry = FileListEntry {
            id: 1,
            tenant_id: Uuid::nil(),
            project_id: Uuid::nil(),
            signal_type: "traces".to_string(),
            stream_name: "default".to_string(),
            date: "2024/01/01/00".to_string(),
            file_path: file_path.to_string_lossy().into_owned(),
            location: "local".to_string(),
            min_ts: 0,
            max_ts: i64::MAX,
            records: 2,
            original_size: 0,
            compressed_size: 0,
            deleted: false,
            created_at: 0,
            updated_at: 0,
        };

        struct SingleFileRepo(FileListEntry);

        #[async_trait::async_trait]
        impl FileListRepository for SingleFileRepo {
            async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
                Ok(1)
            }

            async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
                Ok(vec![self.0.clone()])
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

        let reader =
            ParquetFileReader::new(dir.path().to_path_buf(), Arc::new(SingleFileRepo(entry)));

        let batches = reader
            .query_parquet(
                FileListFilter::default(),
                "SELECT trace_id, status_code FROM spans ORDER BY trace_id",
            )
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
