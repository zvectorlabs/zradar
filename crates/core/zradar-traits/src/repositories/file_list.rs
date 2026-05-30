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

    async fn sum_compressed_size(&self, filter: FileListFilter) -> anyhow::Result<i64> {
        let files = self.query_files(filter).await?;
        Ok(files.into_iter().fold(0_i64, |total, file| {
            total.saturating_add(file.compressed_size.max(0))
        }))
    }

    async fn query_compactable_groups(
        &self,
        cutoff_us: i64,
    ) -> anyhow::Result<Vec<Vec<FileListEntry>>> {
        let files = self
            .query_files(FileListFilter {
                location: Some("local".to_string()),
                deleted: Some(false),
                ..Default::default()
            })
            .await?;
        let mut groups =
            std::collections::HashMap::<(Uuid, Uuid, String, String), Vec<FileListEntry>>::new();
        for file in files {
            if file.created_at < cutoff_us {
                groups
                    .entry((
                        file.tenant_id,
                        file.project_id,
                        file.signal_type.clone(),
                        file.date.clone(),
                    ))
                    .or_default()
                    .push(file);
            }
        }
        Ok(groups
            .into_values()
            .filter(|group| group.len() >= 2)
            .collect())
    }

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

    /// Return the distinct (tenant_id, project_id, signal_type) combinations for
    /// all non-deleted files whose `created_at` is before `before_micros`.
    ///
    /// Used by the storage-usage snapshot job to discover which keys need a
    /// snapshot without loading every file row.
    async fn list_active_keys(
        &self,
        before_micros: i64,
    ) -> anyhow::Result<Vec<(Uuid, Uuid, String)>> {
        // Default implementation falls back to query_files — works for tests /
        // in-memory repos without needing a separate SQL method.
        let files = self
            .query_files(FileListFilter {
                deleted: Some(false),
                ..Default::default()
            })
            .await?;
        let mut seen = std::collections::HashSet::new();
        let mut keys = Vec::new();
        for f in files {
            if f.created_at < before_micros
                && seen.insert((f.tenant_id, f.project_id, f.signal_type.clone()))
            {
                keys.push((f.tenant_id, f.project_id, f.signal_type));
            }
        }
        Ok(keys)
    }

    /// Check whether a file produced from a WAL flush at the given offset already
    /// exists in the file_list. Used by WAL replay to skip duplicate flushes.
    async fn already_flushed(
        &self,
        _tenant_id: Uuid,
        _project_id: Uuid,
        _signal_type: &str,
        _stream_name: &str,
        _max_wal_offset: i64,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
}
