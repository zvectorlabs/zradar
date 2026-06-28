//! M07-07: Background Parquet compaction job.
//!
//! `Compactor` periodically scans the `file_list` for small Parquet files that
//! share the same `(workspace_id, signal_type, date)` bucket, merges
//! their rows into a single larger file, soft-deletes the originals, and
//! registers the merged result.
//!
//! ## Why compaction?
//!
//! Under write-buffered ingestion (M07-04) each flush produces one Parquet
//! file per `(workspace, signal, hour)` slot.  Over a long-running
//! deployment, a single day-bucket can accumulate dozens of small files,
//! each requiring a separate file-open + footer-parse during queries.
//! Compaction collapses these into one file so DataFusion's `ListingTable`
//! (M07-05) only has to open a single file per bucket.
//!
//! ## Algorithm
//!
//! 1. Query all non-deleted local files from `file_list`.
//! 2. Group by `(workspace_id, signal_type, date)`.
//! 3. For groups with ≥ `min_files` files **where every file is smaller than
//!    `max_file_size_bytes`**, schedule a merge.
//! 4. For each merge candidate group: read all files into Arrow `RecordBatch`
//!    slices, concatenate, write a single new Parquet file via
//!    `ParquetFileWriter`, soft-delete originals (`deleted=true`). Physical
//!    bytes are reclaimed later by `FileReclaimer` in `zradar-retention`.
//!
//! CPU-bound Parquet I/O is offloaded to `spawn_blocking`.

use std::sync::Arc;

use anyhow::Context;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use zradar_models::FileListEntry;
use zradar_traits::FileListRepository;

use crate::writer::ParquetFileWriter;

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

/// Background job that merges small Parquet files within the same date bucket.
pub struct Compactor {
    file_list_repo: Arc<dyn FileListRepository>,
    writer: Arc<ParquetFileWriter>,
    /// How often (seconds) the job wakes up to scan for merge candidates.
    check_interval_secs: u64,
    /// Minimum files in a bucket before compaction is triggered.
    min_files: usize,
    /// Only compact files strictly smaller than this byte threshold.
    max_file_size_bytes: i64,
}

impl Compactor {
    /// Create a new `Compactor`.
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        writer: Arc<ParquetFileWriter>,
        check_interval_secs: u64,
        min_files: usize,
        max_file_size_bytes: i64,
    ) -> Self {
        Self {
            file_list_repo,
            writer,
            check_interval_secs,
            min_files,
            max_file_size_bytes,
        }
    }

    /// Run the compaction loop until `cancel` fires.
    ///
    /// Intended to be spawned with `tokio::spawn`.
    pub async fn run(self, cancel: CancellationToken) {
        let mut tick = interval(Duration::from_secs(self.check_interval_secs));

        info!(
            check_interval_secs = self.check_interval_secs,
            min_files = self.min_files,
            max_file_size_bytes = self.max_file_size_bytes,
            "Compactor started"
        );

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    debug!("Compactor: running compaction scan");
                    if let Err(e) = self.compact_all().await {
                        error!(error = %e, "Compactor: scan failed");
                    }
                }
                _ = cancel.cancelled() => {
                    info!("Compactor: shutdown");
                    return;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal: scan + compact
    // -----------------------------------------------------------------------

    async fn compact_all(&self) -> anyhow::Result<()> {
        let cutoff_us = chrono::Utc::now().timestamp_micros()
            - (self.check_interval_secs as i64).saturating_mul(1_000_000);
        let groups = self
            .file_list_repo
            .query_compactable_groups(cutoff_us)
            .await
            .context("Compactor: failed to query compactable groups")?;

        let mut merged = 0usize;
        for files in groups {
            if files.is_empty() {
                continue;
            }
            let bucket = (
                files[0].workspace_id.to_string(),
                files[0].signal_type.clone(),
                files[0].date.clone(),
            );
            // Only compact when we have enough small files.
            let small: Vec<_> = files
                .iter()
                .filter(|f| f.compressed_size < self.max_file_size_bytes)
                .collect();

            if small.len() < self.min_files {
                continue;
            }

            debug!(
                workspace = %bucket.0,
                signal = %bucket.1,
                date = %bucket.2,
                file_count = small.len(),
                "Compactor: merging bucket"
            );

            match self.compact_group(&bucket, &small).await {
                Ok(()) => merged += 1,
                Err(e) => {
                    warn!(
                        workspace = %bucket.0,
                        signal = %bucket.1,
                        date = %bucket.2,
                        error = %e,
                        "Compactor: failed to compact group — skipping"
                    );
                }
            }
        }

        if merged > 0 {
            info!(groups_compacted = merged, "Compactor: scan complete");
        }

        Ok(())
    }

    /// Read all files in `files`, merge their RecordBatches, write one new
    /// Parquet file, and soft-delete the originals.
    async fn compact_group(
        &self,
        bucket: &(String, String, String),
        files: &[&FileListEntry],
    ) -> anyhow::Result<()> {
        let (workspace_id, signal_type, _date) = bucket;

        // Collect file paths for blocking read.
        let paths: Vec<String> = files.iter().map(|f| f.file_path.clone()).collect();
        let stream_name = files[0].stream_name.clone();

        // Read all Parquet files → RecordBatches in a spawn_blocking task.
        let batches = tokio::task::spawn_blocking(move || read_parquet_files(&paths))
            .await
            .context("spawn_blocking panicked in compact_group")?
            .context("Failed to read Parquet files for compaction")?;

        if batches.is_empty() {
            return Ok(());
        }

        // Concatenate all batches into one.
        let schema = batches[0].schema();
        let merged_batch = arrow::compute::concat_batches(&schema, &batches)
            .context("Failed to concatenate RecordBatches for compaction")?;

        let record_count = merged_batch.num_rows() as i64;

        // Compute merged time range.
        let min_ts_us = files.iter().map(|f| f.min_ts).min().unwrap_or(0);
        let max_ts_us = files.iter().map(|f| f.max_ts).max().unwrap_or(0);

        // Write merged file via the shared writer.
        self.writer
            .write_record_batch(
                workspace_id,
                &stream_name,
                signal_type,
                merged_batch,
                min_ts_us * 1_000, // convert µs → ns for write_record_batch
                max_ts_us * 1_000,
                record_count,
            )
            .await
            .context("Compactor: failed to write merged Parquet file")?;

        // Soft-delete the original files: flip `deleted=true` so queries stop
        // seeing them immediately. We never unlink here — the lease-aware
        // FileReclaimer (zradar-retention) sweeps `deleted=true` rows and is the
        // sole owner of physical removal, so a query still reading an original
        // (and holding a lease) can finish before its bytes are reclaimed.
        let ids: Vec<i64> = files.iter().map(|f| f.id).collect();
        self.file_list_repo
            .mark_deleted(&ids)
            .await
            .context("Compactor: failed to mark original files as deleted")?;

        debug!(
            files_merged = ids.len(),
            records = record_count,
            "Compactor: group merged successfully"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Group key: `(workspace_id, signal_type, date)`.
#[cfg(test)]
type BucketKey = (String, String, String);

#[cfg(test)]
fn group_by_bucket(
    files: Vec<FileListEntry>,
) -> std::collections::HashMap<BucketKey, Vec<FileListEntry>> {
    use std::collections::HashMap;

    let mut groups: HashMap<BucketKey, Vec<FileListEntry>> = HashMap::new();
    for file in files {
        let key = (
            file.workspace_id.to_string(),
            file.signal_type.clone(),
            file.date.clone(),
        );
        groups.entry(key).or_default().push(file);
    }
    groups
}

/// Read a list of Parquet files from disk and return all rows as `RecordBatch`es.
///
/// This function is CPU-bound and must be called inside `spawn_blocking`.
fn read_parquet_files(paths: &[String]) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut all_batches = Vec::new();
    for path in paths {
        let file = std::fs::File::open(path)
            .with_context(|| format!("Compactor: failed to open {path}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("Compactor: failed to build reader for {path}"))?;
        let reader = builder
            .build()
            .with_context(|| format!("Compactor: failed to build Parquet reader for {path}"))?;
        for batch in reader {
            all_batches.push(
                batch.with_context(|| format!("Compactor: error reading batch from {path}"))?,
            );
        }
    }
    Ok(all_batches)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use crate::writer::{ParquetFileWriter, WriterConfig};
    use std::sync::Mutex;
    use uuid::Uuid;
    use zradar_models::{FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate};
    use zradar_traits::FileListRepository;

    // ---- stub repo ----

    #[derive(Default)]
    struct TrackingRepo {
        files: Mutex<Vec<FileListEntry>>,
        registered: Mutex<Vec<NewFileListEntry>>,
        deleted: Mutex<Vec<i64>>,
    }

    impl TrackingRepo {
        fn with_files(files: Vec<FileListEntry>) -> Self {
            Self {
                files: Mutex::new(files),
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl FileListRepository for TrackingRepo {
        async fn register_file(&self, e: NewFileListEntry) -> anyhow::Result<i64> {
            self.registered.lock().unwrap().push(e);
            Ok(99)
        }
        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(self.files.lock().unwrap().clone())
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, ids: &[i64]) -> anyhow::Result<()> {
            self.deleted.lock().unwrap().extend_from_slice(ids);
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(
            &self,
            _: zradar_models::WorkspaceId,
        ) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn make_span_parquet(dir: &std::path::Path, name: &str, trace_id: &str) -> String {
        use crate::schema::spans::spans_to_record_batch;
        use parquet::arrow::ArrowWriter;
        use zradar_models::Span;

        let span = Span {
            trace_id: trace_id.to_string(),
            workspace_id: WorkspaceId::from(uuid::Uuid::nil()).to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            duration_ns: 1_000_000,
            ..Span::default()
        };
        let path = dir.join(name);
        let batch = spans_to_record_batch(&[span]).unwrap();
        let schema = batch.schema();
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn test_compact_group_merges_small_files() {
        let dir = tempfile::TempDir::new().unwrap();

        let workspace_id = Uuid::nil();

        // Create 4 small Parquet files.
        let mut entries = Vec::new();
        for i in 0..4u32 {
            let trace_id = format!("trace-{i:03}");
            let path = make_span_parquet(dir.path(), &format!("{i}.parquet"), &trace_id);
            let meta = std::fs::metadata(&path).unwrap();
            entries.push(FileListEntry {
                id: i as i64 + 1,
                workspace_id: workspace_id.into(),
                signal_type: "traces".to_string(),
                stream_name: "svc".to_string(),
                date: "2024/01/15".to_string(),
                file_path: path,
                location: "local".to_string(),
                min_ts: 1_000_000,
                max_ts: 2_000_000,
                records: 1,
                original_size: meta.len() as i64,
                compressed_size: meta.len() as i64,
                deleted: false,
                created_at: 0,
                updated_at: 0,
            });
        }

        let repo = Arc::new(TrackingRepo::with_files(entries.clone()));
        let config = WriterConfig {
            bloom_filter_columns: vec![],
            fsync_before_rename: false,
        };
        let writer = Arc::new(ParquetFileWriter::with_config(
            dir.path().to_path_buf(),
            repo.clone() as Arc<dyn FileListRepository>,
            config,
        ));

        let compactor = Compactor::new(
            repo.clone() as Arc<dyn FileListRepository>,
            writer,
            3600,
            4,                  // min_files
            1024 * 1024 * 1024, // max_file_size_bytes: 1 GiB (always compact)
        );

        compactor.compact_all().await.unwrap();

        // The 4 originals should be marked deleted.
        let deleted = repo.deleted.lock().unwrap().clone();
        assert_eq!(
            deleted.len(),
            4,
            "all 4 original files must be soft-deleted"
        );

        // One merged file should be registered.
        let registered = repo.registered.lock().unwrap().clone();
        assert_eq!(
            registered.len(),
            1,
            "exactly 1 merged file must be registered"
        );
        assert_eq!(
            registered[0].records, 4,
            "merged file must contain all 4 rows"
        );
    }

    #[tokio::test]
    async fn test_compact_skips_group_below_min_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace_id = Uuid::nil();

        // Only 2 files — below the min_files threshold of 4.
        let mut entries = Vec::new();
        for i in 0..2u32 {
            let path = make_span_parquet(dir.path(), &format!("{i}.parquet"), &format!("t-{i}"));
            entries.push(FileListEntry {
                id: i as i64 + 1,
                workspace_id: workspace_id.into(),
                signal_type: "traces".to_string(),
                stream_name: "svc".to_string(),
                date: "2024/01/15".to_string(),
                file_path: path,
                location: "local".to_string(),
                min_ts: 0,
                max_ts: 0,
                records: 1,
                original_size: 100,
                compressed_size: 100,
                deleted: false,
                created_at: 0,
                updated_at: 0,
            });
        }

        let repo = Arc::new(TrackingRepo::with_files(entries));
        let writer = Arc::new(ParquetFileWriter::new(
            dir.path().to_path_buf(),
            repo.clone() as Arc<dyn FileListRepository>,
        ));

        let compactor = Compactor::new(
            repo.clone() as Arc<dyn FileListRepository>,
            writer,
            3600,
            4, // min_files = 4, but we only have 2
            1024 * 1024 * 1024,
        );

        compactor.compact_all().await.unwrap();

        // Nothing should be compacted.
        assert!(
            repo.deleted.lock().unwrap().is_empty(),
            "no files should be deleted"
        );
        assert!(
            repo.registered.lock().unwrap().is_empty(),
            "no files should be registered"
        );
    }

    #[test]
    fn test_group_by_bucket_groups_correctly() {
        let workspace = Uuid::nil();

        let make = |date: &str| FileListEntry {
            id: 1,
            workspace_id: workspace.into(),
            signal_type: "traces".to_string(),
            stream_name: "svc".to_string(),
            date: date.to_string(),
            file_path: "/tmp/x".to_string(),
            location: "local".to_string(),
            min_ts: 0,
            max_ts: 0,
            records: 0,
            original_size: 0,
            compressed_size: 0,
            deleted: false,
            created_at: 0,
            updated_at: 0,
        };

        let files = vec![make("2024/01/15"), make("2024/01/15"), make("2024/01/16")];
        let groups = group_by_bucket(files);

        assert_eq!(groups.len(), 2, "two distinct date buckets");
        let jan15_key = (
            workspace.to_string(),
            "traces".to_string(),
            "2024/01/15".to_string(),
        );
        assert_eq!(groups[&jan15_key].len(), 2);
    }
}
