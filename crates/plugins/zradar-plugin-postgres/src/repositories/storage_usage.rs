use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use chrono::NaiveDate;
use sqlx::{Executor, Postgres, QueryBuilder, Row};
use std::sync::Arc;
use zradar_models::WorkspaceId;
use zradar_traits::{StorageUsageDailySnapshot, StorageUsageDelta, StorageUsageRepository};

pub struct PostgresStorageUsageRepository {
    client: Arc<PostgresClient>,
}

impl PostgresStorageUsageRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StorageUsageRepository for PostgresStorageUsageRepository {
    async fn record_cleanup_daily(&self, deltas: &[StorageUsageDelta]) -> anyhow::Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        upsert_cleanup_deltas(self.client.pool(), deltas).await
    }

    async fn record_cleanup_and_delete(
        &self,
        deltas: &[StorageUsageDelta],
        file_ids: &[i64],
    ) -> anyhow::Result<()> {
        if deltas.is_empty() && file_ids.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .client
            .pool()
            .begin()
            .await
            .context("Failed to begin cleanup transaction")?;

        if !deltas.is_empty() {
            upsert_cleanup_deltas(&mut *tx, deltas).await?;
        }

        if !file_ids.is_empty() {
            sqlx::query("DELETE FROM file_list WHERE id = ANY($1)")
                .bind(file_ids)
                .execute(&mut *tx)
                .await
                .context("Failed to delete file_list entries")?;
        }

        tx.commit()
            .await
            .context("Failed to commit cleanup transaction")?;

        Ok(())
    }

    async fn get_ingestion_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(compressed_bytes, 0)::bigint AS compressed_bytes,
                COALESCE(file_count, 0)::bigint       AS file_count
            FROM ingestion_daily
            WHERE workspace_id = $1
              AND signal_kind = $2
              AND day         = $3
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(day)
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to fetch ingestion_daily")?;

        Ok(row
            .map(|r| {
                (
                    r.get::<i64, _>("compressed_bytes"),
                    r.get::<i64, _>("file_count"),
                )
            })
            .unwrap_or((0, 0)))
    }

    async fn get_cleanup_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(compressed_bytes, 0)::bigint AS compressed_bytes,
                COALESCE(file_count, 0)::bigint       AS file_count
            FROM storage_cleanup_daily
            WHERE workspace_id = $1
              AND signal_kind = $2
              AND day         = $3
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(day)
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to fetch storage_cleanup_daily")?;

        Ok(row
            .map(|r| {
                (
                    r.get::<i64, _>("compressed_bytes"),
                    r.get::<i64, _>("file_count"),
                )
            })
            .unwrap_or((0, 0)))
    }

    async fn get_previous_snapshot(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<Option<(i64, i64)>> {
        let prev_day = day - chrono::Duration::days(1);
        let row = sqlx::query(
            r#"
            SELECT compressed_bytes, file_count
            FROM retention_storage_daily
            WHERE workspace_id = $1
              AND signal_kind = $2
              AND bucket_index = 0
              AND day = $3
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(prev_day)
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to fetch previous snapshot")?;

        Ok(row.map(|r| {
            (
                r.get::<i64, _>("compressed_bytes"),
                r.get::<i64, _>("file_count"),
            )
        }))
    }

    async fn upsert_storage_snapshot(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
        compressed_bytes: i64,
        file_count: i64,
    ) -> anyhow::Result<()> {
        let captured_at = chrono::Utc::now().timestamp_micros();
        sqlx::query(
            r#"
            INSERT INTO retention_storage_daily
                (workspace_id, signal_kind, bucket_index, day,
                 compressed_bytes, file_count, captured_at)
            VALUES ($1, $2, 0, $3, $4, $5, $6)
            ON CONFLICT (workspace_id, signal_kind, bucket_index, day)
            DO UPDATE SET
                compressed_bytes = EXCLUDED.compressed_bytes,
                file_count       = EXCLUDED.file_count,
                captured_at      = EXCLUDED.captured_at
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(day)
        .bind(compressed_bytes)
        .bind(file_count)
        .bind(captured_at)
        .execute(self.client.pool())
        .await
        .context("Failed to upsert storage snapshot")?;

        Ok(())
    }

    async fn get_current_file_stats(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        before_micros: i64,
    ) -> anyhow::Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(compressed_size), 0)::bigint AS compressed_bytes,
                COUNT(*)::bigint AS file_count
            FROM file_list
            WHERE workspace_id = $1
              AND signal_type = $2
              AND deleted     = false
              AND created_at  < $3
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(before_micros)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to get current file stats")?;

        Ok((
            row.get::<i64, _>("compressed_bytes"),
            row.get::<i64, _>("file_count"),
        ))
    }

    async fn query_storage_usage_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: Option<&str>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> anyhow::Result<Vec<StorageUsageDailySnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT
                workspace_id,
                signal_kind,
                day::text AS day,
                compressed_bytes,
                file_count,
                captured_at,
                false AS estimated_today
            FROM retention_storage_daily
            WHERE workspace_id = $1
              AND bucket_index = 0
              AND day < CURRENT_DATE
              AND ($2::text IS NULL OR signal_kind = $2)
              AND ($3::bigint IS NULL OR day >= (to_timestamp($3::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
              AND ($4::bigint IS NULL OR day <= (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
            ORDER BY day DESC, signal_kind
            "#,
        )
        .bind(workspace_id.into_inner())
        .bind(signal_kind)
        .bind(start_micros)
        .bind(end_micros)
        .fetch_all(self.client.pool())
        .await
        .context("Failed to query storage usage daily snapshots")?;

        let mut snapshots = rows
            .into_iter()
            .map(storage_usage_daily_snapshot_from_row)
            .collect::<Vec<_>>();

        let today = chrono::Utc::now().date_naive();
        if includes_day(start_micros, end_micros, today) {
            let today_rows =
                derive_storage_usage_daily_rows(&self.client, workspace_id, signal_kind, today)
                    .await
                    .context("Failed to derive today's storage usage snapshot")?;

            snapshots.extend(
                today_rows
                    .into_iter()
                    .map(storage_usage_daily_snapshot_from_row),
            );
        }

        snapshots.sort_by(|a, b| {
            b.day
                .cmp(&a.day)
                .then_with(|| a.signal_kind.cmp(&b.signal_kind))
        });
        Ok(snapshots)
    }
}

/// Upsert cleanup deltas into `storage_cleanup_daily`.
///
/// Accepts any [`Executor`] so it can run inside or outside a transaction.
/// On conflict it accumulates the delta (additive upsert), making it safe to
/// retry: a duplicate call for the same `(workspace_id, signal, day)` key
/// simply adds to the existing row rather than double-counting — callers must
/// ensure they do not call this more than once per cleanup batch for the same
/// key without also retrying the matching `delete_entries`.
async fn upsert_cleanup_deltas<'e, E>(
    executor: E,
    deltas: &[StorageUsageDelta],
) -> anyhow::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let now = chrono::Utc::now().timestamp_micros();
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO storage_cleanup_daily (
            workspace_id,
            signal_kind,
            day,
            compressed_bytes,
            file_count,
            updated_at
        )
        "#,
    );

    builder.push_values(deltas, |mut row, delta| {
        row.push_bind(delta.workspace_id)
            .push_bind(&delta.signal_kind)
            .push_bind(delta.day)
            .push_bind(delta.compressed_bytes.max(0))
            .push_bind(delta.file_count.max(0))
            .push_bind(now);
    });

    builder.push(
        r#"
        ON CONFLICT (workspace_id, signal_kind, day)
        DO UPDATE SET
            compressed_bytes = storage_cleanup_daily.compressed_bytes
                + EXCLUDED.compressed_bytes,
            file_count = storage_cleanup_daily.file_count + EXCLUDED.file_count,
            updated_at = GREATEST(storage_cleanup_daily.updated_at, EXCLUDED.updated_at)
        "#,
    );

    builder
        .build()
        .execute(executor)
        .await
        .context("Failed to upsert storage cleanup daily deltas")?;

    Ok(())
}

fn storage_usage_daily_snapshot_from_row(row: sqlx::postgres::PgRow) -> StorageUsageDailySnapshot {
    StorageUsageDailySnapshot {
        workspace_id: row.get("workspace_id"),
        signal_kind: row.get("signal_kind"),
        day: row.get("day"),
        compressed_bytes: row.get("compressed_bytes"),
        file_count: row.get("file_count"),
        captured_at: row.get("captured_at"),
        estimated_today: row.get("estimated_today"),
    }
}

fn includes_day(
    start_micros: Option<i64>,
    end_micros: Option<i64>,
    day: chrono::NaiveDate,
) -> bool {
    let starts_before_or_on_day = start_micros
        .and_then(chrono::DateTime::from_timestamp_micros)
        .map(|datetime| datetime.date_naive() <= day)
        .unwrap_or(true);
    let ends_after_or_on_day = end_micros
        .and_then(chrono::DateTime::from_timestamp_micros)
        .map(|datetime| datetime.date_naive() >= day)
        .unwrap_or(true);
    starts_before_or_on_day && ends_after_or_on_day
}

pub async fn derive_storage_usage_daily_rows(
    client: &PostgresClient,
    workspace_id: WorkspaceId,
    signal_filter: Option<&str>,
    day: NaiveDate,
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    sqlx::query(
        r#"
        WITH params AS (
            SELECT $3::date AS snapshot_day,
                   ($3::date - INTERVAL '1 day')::date AS previous_day,
                   $4::bigint AS captured_at
        ),
        previous AS (
            SELECT r.workspace_id,
                   r.signal_kind,
                   r.compressed_bytes,
                   r.file_count
            FROM retention_storage_daily r, params p
            WHERE r.workspace_id = $1
              AND r.bucket_index = 0
              AND r.day = p.previous_day
              AND ($2::text IS NULL OR r.signal_kind = $2)
        ),
        added AS (
            SELECT workspace_id,
                   signal_kind,
                   compressed_bytes,
                   file_count
            FROM ingestion_daily i, params p
            WHERE i.workspace_id = $1
              AND i.day = p.snapshot_day
              AND ($2::text IS NULL OR i.signal_kind = $2)
        ),
        removed AS (
            SELECT workspace_id,
                   signal_kind,
                   compressed_bytes,
                   file_count
            FROM storage_cleanup_daily c, params p
            WHERE c.workspace_id = $1
              AND c.day = p.snapshot_day
              AND ($2::text IS NULL OR c.signal_kind = $2)
        ),
        incremental AS (
            SELECT p.workspace_id,
                   p.signal_kind,
                   GREATEST(
                       COALESCE(p.compressed_bytes, 0)
                       + COALESCE(a.compressed_bytes, 0)
                       - COALESCE(r.compressed_bytes, 0),
                       0
                   )::bigint AS compressed_bytes,
                   GREATEST(
                       COALESCE(p.file_count, 0)
                       + COALESCE(a.file_count, 0)
                       - COALESCE(r.file_count, 0),
                       0
                   )::bigint AS file_count
            FROM previous p
            LEFT JOIN added a USING (workspace_id, signal_kind)
            LEFT JOIN removed r USING (workspace_id, signal_kind)
        ),
        bootstrap AS (
            SELECT f.workspace_id,
                   f.signal_type AS signal_kind,
                   COALESCE(SUM(f.compressed_size), 0)::bigint AS compressed_bytes,
                   COUNT(*)::bigint AS file_count
            FROM file_list f
            LEFT JOIN previous p ON f.workspace_id = p.workspace_id 
                                AND f.signal_type = p.signal_kind
            WHERE f.workspace_id = $1
              AND f.deleted = false
              AND ($2::text IS NULL OR f.signal_type = $2)
              AND p.workspace_id IS NULL
            GROUP BY f.workspace_id, f.signal_type
        ),
        source AS (
            SELECT * FROM incremental
            UNION ALL
            SELECT * FROM bootstrap
        )
        SELECT s.workspace_id,
               s.signal_kind,
               p.snapshot_day::text AS day,
               s.compressed_bytes,
               s.file_count,
               p.captured_at,
               true AS estimated_today
        FROM source s, params p
        ORDER BY s.signal_kind
        "#,
    )
    .bind(workspace_id.into_inner())
    .bind(signal_filter)
    .bind(day)
    .bind(chrono::Utc::now().timestamp_micros())
    .fetch_all(client.pool())
    .await
}
