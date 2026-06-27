//! Parquet file reader backed by DataFusion.
//!
//! `ParquetFileReader` accepts a `FileListFilter` (for coarse file pruning via
//! the `file_list` metadata table) and a SQL string (for fine-grained row
//! filtering and aggregation). DataFusion executes the SQL against the Parquet
//! files and returns `RecordBatch` results with automatic predicate pushdown.
//!
//! ## M07-05: ListingTable (no more UNION ALL)
//!
//! Previously, each matching file was registered individually as `spans_0`,
//! `spans_1`, … and joined with a `UNION ALL` view.  The optimizer had to
//! process each union node separately — O(N × rules) per query.
//!
//! Now a single DataFusion [`ListingTable`] is registered over all matching
//! file URLs.  The planner sees one logical table and applies predicate
//! pushdown to all files simultaneously — O(rules), regardless of N.
//!
//! The `SessionContext` is created fresh per query (via `SharedEngine`) so
//! concurrent queries never see each other's registered tables.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use zradar_models::FileListFilter;
use zradar_traits::{FileLeaseRegistry, FileListRepository};

use crate::disk_cache::DiskCache;
use crate::engine::SharedEngine;
use crate::memory_cache::MemoryCache;

/// Queries Parquet files stored on local disk or S3.
///
/// Supply an `Option<Arc<DiskCache>>` when S3 is configured so that remote
/// files are transparently cached before DataFusion reads them.
pub struct ParquetFileReader {
    /// Root directory where local Parquet files are stored.
    _data_dir: PathBuf,
    file_list_repo: Arc<dyn FileListRepository>,
    /// Optional disk cache for S3-backed files.
    disk_cache: Option<Arc<DiskCache>>,
    /// Optional memory cache for hot local/S3 file bytes.
    memory_cache: Option<Arc<MemoryCache>>,
    /// Pre-configured DataFusion session factory (M07-05).
    engine: SharedEngine,
    /// Optional file-lease registry. When set, every file the reader is about
    /// to scan is leased for the duration of the scan, and background jobs
    /// (FileMover, Compactor, retention) skip leased files.
    lease_registry: Option<Arc<FileLeaseRegistry>>,
}

impl ParquetFileReader {
    /// Create a new reader (local-only, no disk cache).
    pub fn new(data_dir: PathBuf, file_list_repo: Arc<dyn FileListRepository>) -> Self {
        Self {
            _data_dir: data_dir,
            file_list_repo,
            disk_cache: None,
            memory_cache: None,
            engine: SharedEngine::new(),
            lease_registry: None,
        }
    }

    /// Create a reader with an S3 disk cache.
    pub fn with_cache(
        data_dir: PathBuf,
        file_list_repo: Arc<dyn FileListRepository>,
        disk_cache: Arc<DiskCache>,
    ) -> Self {
        Self {
            _data_dir: data_dir,
            file_list_repo,
            disk_cache: Some(disk_cache),
            memory_cache: None,
            engine: SharedEngine::new(),
            lease_registry: None,
        }
    }

    pub fn with_cache_and_memory_cache(
        data_dir: PathBuf,
        file_list_repo: Arc<dyn FileListRepository>,
        disk_cache: Arc<DiskCache>,
        memory_cache: Arc<MemoryCache>,
    ) -> Self {
        Self {
            _data_dir: data_dir,
            file_list_repo,
            disk_cache: Some(disk_cache),
            memory_cache: Some(memory_cache),
            engine: SharedEngine::new(),
            lease_registry: None,
        }
    }

    pub fn with_memory_cache(
        data_dir: PathBuf,
        file_list_repo: Arc<dyn FileListRepository>,
        memory_cache: Arc<MemoryCache>,
    ) -> Self {
        Self {
            _data_dir: data_dir,
            file_list_repo,
            disk_cache: None,
            memory_cache: Some(memory_cache),
            engine: SharedEngine::new(),
            lease_registry: None,
        }
    }

    /// Install a [`FileLeaseRegistry`] so that every file scanned by this
    /// reader is leased for the duration of the scan. Background jobs that
    /// consult the same registry will skip files with active leases, avoiding
    /// move/delete races against in-flight queries.
    pub fn with_lease_registry(mut self, registry: Arc<FileLeaseRegistry>) -> Self {
        self.lease_registry = Some(registry);
        self
    }

    /// Execute `sql` against the Parquet files that match `filter`.
    ///
    /// The SQL must reference a table called `spans`.  Internally the reader
    /// registers all matching files as a single DataFusion `ListingTable`
    /// named `spans`, then executes the provided SQL.
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
    /// This generalises `query_parquet` so callers can query `"metrics"` or
    /// `"logs"` files with the same infrastructure.
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

        // Acquire a read lease on every file we are about to scan. The leases
        // are held in `_file_leases` (RAII) and released when this function
        // returns. The FileMover (local→S3 promotion) and the FileReclaimer
        // (the single physical-deletion chokepoint) consult the same registry
        // and skip leased files, so an in-flight query cannot race a move or a
        // delete.
        //
        // Lease ordering is deliberate: we acquire here *synchronously*,
        // immediately after `query_files` returns and before any `.await`
        // below. That closes the window where the reclaimer could see the file
        // unleased and unlink it after we listed it. The only residual is a
        // sub-microsecond cross-thread gap between the list and this `acquire`,
        // and even then both fallbacks below (local: skip-if-missing at the
        // `exists()` check; S3: scans the DiskCache-local copy, which the
        // reclaimer never deletes) degrade gracefully rather than erroring —
        // so a DataFusion-level scan retry (3a) is not required.
        let _file_leases = self.lease_registry.as_ref().map(|registry| {
            let ids: Vec<i64> = files.iter().map(|f| f.id).collect();
            registry.acquire(&ids)
        });

        // Resolve each file to a local path (fetch from S3 via DiskCache if needed).
        let mut resolved_paths: Vec<String> = Vec::with_capacity(files.len());
        for file in &files {
            let local_path = if file.location == "s3" {
                let s3_key = extract_s3_key(&file.file_path);
                match &self.disk_cache {
                    Some(cache) => cache
                        .get_or_fetch(s3_key)
                        .await
                        .with_context(|| format!("Failed to fetch S3 file: {}", file.file_path))?
                        .to_string_lossy()
                        .into_owned(),
                    None => {
                        return Err(anyhow::anyhow!(
                            "File {} is in S3 but no DiskCache is configured",
                            file.file_path
                        ));
                    }
                }
            } else {
                // Normalize relative paths to absolute for DataFusion ListingTable.
                // If path starts with './', resolve to absolute path.
                if file.file_path.starts_with("./") {
                    std::fs::canonicalize(&file.file_path)
                        .with_context(|| format!("Failed to resolve path: {}", file.file_path))?
                        .to_string_lossy()
                        .into_owned()
                } else {
                    file.file_path.clone()
                }
            };
            if file.location != "s3" && !Path::new(&local_path).exists() {
                tracing::warn!(
                    file_path = %local_path,
                    "Skipping stale file_list entry for missing local Parquet file"
                );
                continue;
            }
            self.populate_memory_cache(&local_path).await?;
            resolved_paths.push(local_path);
        }
        if resolved_paths.is_empty() {
            return Ok(vec![]);
        }

        // Build a fresh SessionContext from the shared engine config.
        // Each query gets its own context so concurrent queries never collide.
        let ctx = self.engine.new_context();

        // M07-05: Register all files as a single ListingTable (no UNION ALL).
        // DataFusion plans predicates against all file-level statistics at once,
        // enabling O(1) planning vs O(N) with the old UNION ALL approach.
        let listing_urls: Vec<ListingTableUrl> = resolved_paths
            .iter()
            .map(|p| {
                // Ensure the path has the file:// scheme DataFusion expects.
                let url_str = if p.starts_with('/') {
                    format!("file://{p}")
                } else {
                    format!("file:///{p}") // Windows compat
                };
                ListingTableUrl::parse(&url_str)
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse file URLs for ListingTable")?;

        // Disable StringView (Utf8View) so columns come back as plain StringArray
        // (Utf8), which our record_batch_to_* converters expect. ParquetFormat has
        // its own per-instance flag that is independent of SessionConfig.
        let format = Arc::new(ParquetFormat::default().with_force_view_types(false));
        let listing_opts = ListingOptions::new(format).with_file_extension(".parquet");

        let listing_config = ListingTableConfig::new_with_multi_paths(listing_urls)
            .with_listing_options(listing_opts)
            .infer_schema(&ctx.state())
            .await
            .context("Failed to infer schema from Parquet files")?;

        let table = Arc::new(
            ListingTable::try_new(listing_config).context("Failed to create ListingTable")?,
        );

        ctx.register_table(table_name, table)
            .context("Failed to register ListingTable")?;

        // Execute the caller-provided SQL.
        let df = ctx.sql(sql).await.context("Failed to parse query SQL")?;
        df.collect()
            .await
            .context("Failed to collect query results")
    }

    async fn populate_memory_cache(&self, local_path: &str) -> anyhow::Result<()> {
        if let Some(cache) = &self.memory_cache
            && cache.get(local_path).is_none()
        {
            let bytes = tokio::fs::read(local_path)
                .await
                .with_context(|| format!("Failed to read file for memory cache: {local_path}"))?;
            cache.insert(local_path, bytes::Bytes::from(bytes));
        }
        Ok(())
    }
}

/// Strip the `s3://bucket/` prefix from an S3 URL and return just the key.
///
/// If the string does not start with `s3://`, it is returned unchanged so
/// that plain keys passed directly also work.
fn extract_s3_key(s3_url: &str) -> &str {
    if let Some(without_scheme) = s3_url.strip_prefix("s3://")
        && let Some(slash_pos) = without_scheme.find('/')
    {
        return &without_scheme[slash_pos + 1..];
    }
    s3_url
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

    #[tokio::test]
    async fn query_with_lease_registry_releases_all_leases_on_return() {
        // After every code path (including the empty-files fast return), the
        // registry must have zero active leases. This is the RAII contract.
        let registry = Arc::new(FileLeaseRegistry::new());
        let reader = ParquetFileReader::new(PathBuf::from("/tmp"), Arc::new(EmptyRepo))
            .with_lease_registry(registry.clone());

        let _ = reader
            .query_parquet(FileListFilter::default(), "SELECT * FROM spans")
            .await
            .unwrap();
        assert_eq!(registry.active_lease_count(), 0);
    }

    /// Full round-trip: write a Parquet file, read it back via DataFusion ListingTable.
    #[tokio::test]
    async fn test_round_trip_write_then_query() {
        use crate::schema::spans::spans_to_record_batch;
        use parquet::arrow::ArrowWriter;
        use tempfile::TempDir;
        use zradar_models::Span;

        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.parquet");

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

    /// Verify that two files are correctly merged by ListingTable (not UNION ALL).
    #[tokio::test]
    async fn test_multi_file_listing_table_query() {
        use crate::schema::spans::spans_to_record_batch;
        use parquet::arrow::ArrowWriter;
        use tempfile::TempDir;
        use zradar_models::Span;

        let dir = TempDir::new().unwrap();

        // Write two separate Parquet files, each with one span.
        let make_file = |name: &str, trace_id: &str| -> FileListEntry {
            let span = Span {
                trace_id: trace_id.to_string(),
                project_id: "proj-001".to_string(),
                ..Span::default()
            };
            let path = dir.path().join(name);
            let batch = spans_to_record_batch(&[span]).unwrap();
            let schema = batch.schema();
            let file = std::fs::File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            FileListEntry {
                id: 1,
                tenant_id: Uuid::nil(),
                project_id: Uuid::nil(),
                signal_type: "traces".to_string(),
                stream_name: "default".to_string(),
                date: "2024/01/01/00".to_string(),
                file_path: path.to_string_lossy().into_owned(),
                location: "local".to_string(),
                min_ts: 0,
                max_ts: i64::MAX,
                records: 1,
                original_size: 0,
                compressed_size: 0,
                deleted: false,
                created_at: 0,
                updated_at: 0,
            }
        };

        let e1 = make_file("a.parquet", "trace-aaa");
        let e2 = make_file("b.parquet", "trace-bbb");

        struct TwoFileRepo(Vec<FileListEntry>);
        #[async_trait::async_trait]
        impl FileListRepository for TwoFileRepo {
            async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
                Ok(1)
            }
            async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
                Ok(self.0.clone())
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

        let reader = ParquetFileReader::new(
            dir.path().to_path_buf(),
            Arc::new(TwoFileRepo(vec![e1, e2])),
        );

        let batches = reader
            .query_parquet(
                FileListFilter::default(),
                "SELECT COUNT(*) AS cnt FROM spans",
            )
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "COUNT(*) should return exactly one row");
        // Both spans from both files should be counted.
        use arrow::array::Int64Array;
        let cnt = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2, "ListingTable must see rows from both files");
    }

    #[tokio::test]
    async fn test_query_parquet_populates_memory_cache() {
        use crate::memory_cache::MemoryCache;
        use crate::schema::spans::spans_to_record_batch;
        use parquet::arrow::ArrowWriter;
        use tempfile::TempDir;
        use zradar_models::Span;

        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("cached.parquet");
        let span = Span {
            trace_id: "trace-cached".to_string(),
            project_id: "proj-001".to_string(),
            ..Span::default()
        };
        let batch = spans_to_record_batch(&[span]).unwrap();
        let schema = batch.schema();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_path_string = file_path.to_string_lossy().into_owned();
        let entry = FileListEntry {
            id: 1,
            tenant_id: Uuid::nil(),
            project_id: Uuid::nil(),
            signal_type: "traces".to_string(),
            stream_name: "default".to_string(),
            date: "2024/01/01/00".to_string(),
            file_path: file_path_string.clone(),
            location: "local".to_string(),
            min_ts: 0,
            max_ts: i64::MAX,
            records: 1,
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

        let cache = Arc::new(MemoryCache::new(1024 * 1024, 4));
        assert!(cache.get(&file_path_string).is_none());
        let reader = ParquetFileReader::with_memory_cache(
            dir.path().to_path_buf(),
            Arc::new(SingleFileRepo(entry)),
            cache.clone(),
        );

        let batches = reader
            .query_parquet(FileListFilter::default(), "SELECT trace_id FROM spans")
            .await
            .unwrap();

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(cache.get(&file_path_string).is_some());
    }
}
