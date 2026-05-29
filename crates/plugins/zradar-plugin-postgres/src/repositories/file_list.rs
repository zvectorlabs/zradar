//! PostgreSQL implementation of FileListRepository.

use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{
    FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
};
use zradar_traits::FileListRepository;

/// PostgreSQL-backed file list repository.
pub struct PostgresFileListRepository {
    client: Arc<PostgresClient>,
}

impl PostgresFileListRepository {
    /// Create a new repository using the given Postgres client.
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl FileListRepository for PostgresFileListRepository {
    async fn register_file(&self, entry: NewFileListEntry) -> anyhow::Result<i64> {
        let row = sqlx::query(
            r#"
            INSERT INTO file_list (
                tenant_id, project_id, signal_type, stream_name, date,
                file_path, location, min_ts, max_ts, records,
                original_size, compressed_size, deleted, created_at, updated_at,
                wal_replay_offset
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, false, $13, $14,
                $15
            )
            RETURNING id
            "#,
        )
        .bind(entry.tenant_id)
        .bind(entry.project_id)
        .bind(entry.signal_type)
        .bind(entry.stream_name)
        .bind(entry.date)
        .bind(entry.file_path)
        .bind(entry.location)
        .bind(entry.min_ts)
        .bind(entry.max_ts)
        .bind(entry.records)
        .bind(entry.original_size)
        .bind(entry.compressed_size)
        .bind(entry.created_at)
        .bind(entry.updated_at)
        .bind(entry.wal_replay_offset)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to register file in file_list")?;

        Ok(row.get::<i64, _>("id"))
    }

    async fn query_files(&self, filter: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
        // Use nullable parameters to build a single parameterised query without
        // dynamic SQL. The IS NULL check short-circuits the condition when the
        // filter field is absent.
        //
        // Time range overlap: a file overlaps [start, end] when:
        //   file.max_ts >= start  AND  file.min_ts <= end
        let entries = sqlx::query_as::<_, FileListEntry>(
            r#"
            SELECT
                id, tenant_id, project_id, signal_type, stream_name, date,
                file_path, location, min_ts, max_ts, records,
                original_size, compressed_size, deleted, created_at, updated_at
            FROM file_list
            WHERE ($1::uuid   IS NULL OR tenant_id    = $1)
              AND ($2::uuid   IS NULL OR project_id   = $2)
              AND ($3::text   IS NULL OR signal_type  = $3)
              AND ($4::text   IS NULL OR stream_name  = $4)
              AND ($5::bigint IS NULL OR max_ts       >= $5)
              AND ($6::bigint IS NULL OR min_ts       <= $6)
              AND ($7::text   IS NULL OR location     = $7)
              AND ($8::bool   IS NULL OR deleted      = $8)
            ORDER BY min_ts DESC, created_at DESC
            "#,
        )
        .bind(filter.tenant_id)
        .bind(filter.project_id)
        .bind(filter.signal_type)
        .bind(filter.stream_name)
        .bind(filter.time_range_start)
        .bind(filter.time_range_end)
        .bind(filter.location)
        .bind(filter.deleted)
        .fetch_all(self.client.pool())
        .await
        .context("Failed to query file_list")?;

        Ok(entries)
    }

    async fn sum_compressed_size(&self, filter: FileListFilter) -> anyhow::Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT COALESCE(SUM(compressed_size), 0)::bigint AS compressed_size
            FROM file_list
            WHERE ($1::uuid   IS NULL OR tenant_id    = $1)
              AND ($2::uuid   IS NULL OR project_id   = $2)
              AND ($3::text   IS NULL OR signal_type  = $3)
              AND ($4::text   IS NULL OR stream_name  = $4)
              AND ($5::bigint IS NULL OR max_ts       >= $5)
              AND ($6::bigint IS NULL OR min_ts       <= $6)
              AND ($7::text   IS NULL OR location     = $7)
              AND ($8::bool   IS NULL OR deleted      = $8)
            "#,
        )
        .bind(filter.tenant_id)
        .bind(filter.project_id)
        .bind(filter.signal_type)
        .bind(filter.stream_name)
        .bind(filter.time_range_start)
        .bind(filter.time_range_end)
        .bind(filter.location)
        .bind(filter.deleted)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to sum compressed_size from file_list")?;

        Ok(row.get::<i64, _>("compressed_size"))
    }

    async fn query_compactable_groups(
        &self,
        cutoff_us: i64,
    ) -> anyhow::Result<Vec<Vec<FileListEntry>>> {
        let entries = sqlx::query_as::<_, FileListEntry>(
            r#"
            WITH groups AS (
                SELECT tenant_id, project_id, signal_type, date, count(*) AS cnt
                FROM file_list
                WHERE deleted = false
                  AND location = 'local'
                  AND created_at < $1
                GROUP BY tenant_id, project_id, signal_type, date
                HAVING count(*) >= 2
            )
            SELECT
                f.id, f.tenant_id, f.project_id, f.signal_type, f.stream_name, f.date,
                f.file_path, f.location, f.min_ts, f.max_ts, f.records,
                f.original_size, f.compressed_size, f.deleted, f.created_at, f.updated_at
            FROM file_list f
            JOIN groups g ON f.tenant_id = g.tenant_id
                         AND f.project_id = g.project_id
                         AND f.signal_type = g.signal_type
                         AND f.date = g.date
            WHERE f.deleted = false
              AND f.location = 'local'
              AND f.created_at < $1
            ORDER BY f.tenant_id, f.project_id, f.signal_type, f.date, f.min_ts
            "#,
        )
        .bind(cutoff_us)
        .fetch_all(self.client.pool())
        .await
        .context("Failed to query compactable file groups")?;

        let mut groups =
            std::collections::HashMap::<(Uuid, Uuid, String, String), Vec<FileListEntry>>::new();
        for entry in entries {
            groups
                .entry((
                    entry.tenant_id,
                    entry.project_id,
                    entry.signal_type.clone(),
                    entry.date.clone(),
                ))
                .or_default()
                .push(entry);
        }

        Ok(groups.into_values().collect())
    }

    async fn update_location(&self, id: i64, location: &str, new_path: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp_micros();
        sqlx::query(
            r#"
            UPDATE file_list
            SET location   = $1,
                file_path  = $2,
                updated_at = $3
            WHERE id = $4
            "#,
        )
        .bind(location)
        .bind(new_path)
        .bind(now)
        .bind(id)
        .execute(self.client.pool())
        .await
        .context("Failed to update file_list location")?;

        Ok(())
    }

    async fn mark_deleted(&self, ids: &[i64]) -> anyhow::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp_micros();
        sqlx::query(
            r#"
            UPDATE file_list
            SET deleted    = true,
                updated_at = $1
            WHERE id = ANY($2)
            "#,
        )
        .bind(now)
        .bind(ids)
        .execute(self.client.pool())
        .await
        .context("Failed to mark files as deleted")?;

        Ok(())
    }

    async fn delete_entries(&self, ids: &[i64]) -> anyhow::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM file_list WHERE id = ANY($1)")
            .bind(ids)
            .execute(self.client.pool())
            .await
            .context("Failed to hard-delete file_list entries")?;

        Ok(())
    }

    async fn get_stream_stats(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
    ) -> anyhow::Result<Vec<StreamStats>> {
        let stats = sqlx::query_as::<_, StreamStats>(
            r#"
            SELECT
                id, tenant_id, project_id, signal_type, stream_name,
                file_count, min_ts, max_ts, total_records,
                total_original_size, total_compressed_size, updated_at
            FROM stream_stats
            WHERE tenant_id  = $1
              AND project_id = $2
            ORDER BY signal_type, stream_name
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .fetch_all(self.client.pool())
        .await
        .context("Failed to query stream_stats")?;

        Ok(stats)
    }

    async fn already_flushed(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal_type: &str,
        stream_name: &str,
        max_wal_offset: i64,
    ) -> anyhow::Result<bool> {
        let row = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM file_list
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND signal_type = $3
                  AND stream_name = $4
                  AND wal_replay_offset >= $5
                  AND deleted = false
            )
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(signal_type)
        .bind(stream_name)
        .bind(max_wal_offset)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to check already_flushed")?;

        Ok(row)
    }

    async fn upsert_stream_stats(&self, stats: StreamStatsUpdate) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp_micros();
        sqlx::query(
            r#"
            INSERT INTO stream_stats (
                tenant_id, project_id, signal_type, stream_name,
                file_count, min_ts, max_ts,
                total_records, total_original_size, total_compressed_size,
                updated_at
            ) VALUES (
                $1, $2, $3, $4,
                1, $5, $6,
                $7, $8, $9,
                $10
            )
            ON CONFLICT (tenant_id, project_id, signal_type, stream_name)
            DO UPDATE SET
                file_count            = stream_stats.file_count            + 1,
                min_ts                = LEAST(stream_stats.min_ts,                   EXCLUDED.min_ts),
                max_ts                = GREATEST(stream_stats.max_ts,                EXCLUDED.max_ts),
                total_records         = stream_stats.total_records         + EXCLUDED.total_records,
                total_original_size   = stream_stats.total_original_size   + EXCLUDED.total_original_size,
                total_compressed_size = stream_stats.total_compressed_size + EXCLUDED.total_compressed_size,
                updated_at            = EXCLUDED.updated_at
            "#,
        )
        .bind(stats.tenant_id)
        .bind(stats.project_id)
        .bind(stats.signal_type)
        .bind(stats.stream_name)
        .bind(stats.min_ts)
        .bind(stats.max_ts)
        .bind(stats.records_delta)
        .bind(stats.original_size_delta)
        .bind(stats.compressed_size_delta)
        .bind(now)
        .execute(self.client.pool())
        .await
        .context("Failed to upsert stream_stats")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_file_list_filter_overlap_logic() {
        // A file [min_ts=100, max_ts=200] overlaps query [start=150, end=250]
        // because max_ts(200) >= start(150) AND min_ts(100) <= end(250).
        let file_min = 100_i64;
        let file_max = 200_i64;
        let query_start = 150_i64;
        let query_end = 250_i64;
        assert!(file_max >= query_start && file_min <= query_end);
    }

    #[test]
    fn test_file_list_filter_no_overlap() {
        let file_min = 300_i64;
        let file_max = 400_i64;
        let query_start = 100_i64;
        let query_end = 200_i64;
        assert!(!(file_max >= query_start && file_min <= query_end));
    }
}
