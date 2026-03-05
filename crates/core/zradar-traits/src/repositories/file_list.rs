//! FileListRepository trait — metadata tracking for Parquet files.
//!
//! Every Parquet file written to disk or S3 is registered here.
//! The repository is queried by the read path to find which files
//! overlap a given time range.

use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::{
    FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
};

/// Repository for the `file_list` and `stream_stats` Postgres tables.
#[async_trait]
pub trait FileListRepository: Send + Sync {
    /// Register a newly written Parquet file and return its assigned id.
    async fn register_file(&self, entry: NewFileListEntry) -> anyhow::Result<i64>;

    /// Query files matching the given filter.
    ///
    /// Time range semantics: returns files whose `[min_ts, max_ts]` interval
    /// overlaps the filter's `[time_range_start, time_range_end]`.
    async fn query_files(&self, filter: FileListFilter) -> anyhow::Result<Vec<FileListEntry>>;

    /// Move a file to a new storage location (e.g. local → s3) and update its path.
    async fn update_location(&self, id: i64, location: &str, new_path: &str) -> anyhow::Result<()>;

    /// Soft-delete a set of files (sets `deleted = true`).
    async fn mark_deleted(&self, ids: &[i64]) -> anyhow::Result<()>;

    /// Hard-delete file entries after physical deletion from storage.
    async fn delete_entries(&self, ids: &[i64]) -> anyhow::Result<()>;

    /// Return stream stats for all streams belonging to a tenant + project.
    async fn get_stream_stats(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
    ) -> anyhow::Result<Vec<StreamStats>>;

    /// Upsert stream stats for a single stream (insert or accumulate deltas).
    async fn upsert_stream_stats(&self, stats: StreamStatsUpdate) -> anyhow::Result<()>;
}
