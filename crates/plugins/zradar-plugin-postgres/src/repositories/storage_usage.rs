use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use chrono::NaiveDate;
use sqlx::{Postgres, QueryBuilder, Row};
use std::sync::Arc;
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

        let now = chrono::Utc::now().timestamp_micros();
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            INSERT INTO storage_cleanup_daily (
                tenant_id,
                project_id,
                signal_kind,
                day,
                compressed_bytes,
                file_count,
                updated_at
            )
            "#,
        );

        builder.push_values(deltas, |mut row, delta| {
            row.push_bind(delta.tenant_id)
                .push_bind(delta.project_id)
                .push_bind(&delta.signal_kind)
                .push_bind(delta.day)
                .push_bind(delta.compressed_bytes.max(0))
                .push_bind(delta.file_count.max(0))
                .push_bind(now);
        });

        builder.push(
            r#"
            ON CONFLICT (tenant_id, project_id, signal_kind, day)
            DO UPDATE SET
                compressed_bytes = storage_cleanup_daily.compressed_bytes
                    + EXCLUDED.compressed_bytes,
                file_count = storage_cleanup_daily.file_count + EXCLUDED.file_count,
                updated_at = GREATEST(storage_cleanup_daily.updated_at, EXCLUDED.updated_at)
            "#,
        );

        builder
            .build()
            .execute(self.client.pool())
            .await
            .context("Failed to upsert storage cleanup daily deltas")?;

        Ok(())
    }

    async fn snapshot_storage_daily(&self, day: NaiveDate) -> anyhow::Result<()> {
        let captured_at = chrono::Utc::now().timestamp_micros();
        sqlx::query(
            r#"
            WITH params AS (
                SELECT $1::date AS snapshot_day,
                       ($1::date - INTERVAL '1 day')::date AS previous_day,
                       $2::bigint AS captured_at
            ),
            has_previous AS (
                SELECT EXISTS (
                    SELECT 1
                    FROM retention_storage_daily r, params p
                    WHERE r.bucket_index = 0
                      AND r.day = p.previous_day
                ) AS value
            ),
            previous AS (
                SELECT r.tenant_id,
                       r.project_id,
                       r.signal_kind,
                       r.compressed_bytes,
                       r.file_count
                FROM retention_storage_daily r, params p
                WHERE r.bucket_index = 0
                  AND r.day = p.previous_day
            ),
            added AS (
                SELECT tenant_id,
                       project_id,
                       signal_kind,
                       compressed_bytes,
                       file_count
                FROM ingestion_daily i, params p
                WHERE i.day = p.snapshot_day
            ),
            removed AS (
                SELECT tenant_id,
                       project_id,
                       signal_kind,
                       compressed_bytes,
                       file_count
                FROM storage_cleanup_daily c, params p
                WHERE c.day = p.snapshot_day
            ),
            keys AS (
                SELECT tenant_id, project_id, signal_kind FROM previous
                UNION
                SELECT tenant_id, project_id, signal_kind FROM added
                UNION
                SELECT tenant_id, project_id, signal_kind FROM removed
            ),
            incremental AS (
                SELECT k.tenant_id,
                       k.project_id,
                       k.signal_kind,
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
                FROM keys k
                LEFT JOIN previous p USING (tenant_id, project_id, signal_kind)
                LEFT JOIN added a USING (tenant_id, project_id, signal_kind)
                LEFT JOIN removed r USING (tenant_id, project_id, signal_kind)
                WHERE (SELECT value FROM has_previous)
            ),
            bootstrap AS (
                SELECT tenant_id,
                       project_id,
                       signal_type AS signal_kind,
                       COALESCE(SUM(compressed_size), 0)::bigint AS compressed_bytes,
                       COUNT(*)::bigint AS file_count
                FROM file_list
                WHERE deleted = false
                GROUP BY tenant_id, project_id, signal_type
                HAVING NOT (SELECT value FROM has_previous)
            ),
            source AS (
                SELECT * FROM incremental
                UNION ALL
                SELECT * FROM bootstrap
            )
            INSERT INTO retention_storage_daily (
                tenant_id,
                project_id,
                signal_kind,
                bucket_index,
                day,
                compressed_bytes,
                file_count,
                captured_at
            )
            SELECT s.tenant_id,
                   s.project_id,
                   s.signal_kind,
                   0,
                   p.snapshot_day,
                   s.compressed_bytes,
                   s.file_count,
                   p.captured_at
            FROM source s, params p
            ON CONFLICT (tenant_id, project_id, signal_kind, bucket_index, day)
            DO UPDATE SET
                compressed_bytes = EXCLUDED.compressed_bytes,
                file_count = EXCLUDED.file_count,
                captured_at = EXCLUDED.captured_at
            "#,
        )
        .bind(day)
        .bind(captured_at)
        .execute(self.client.pool())
        .await
        .context("Failed to snapshot storage usage daily")?;

        Ok(())
    }

    async fn query_storage_usage_daily(
        &self,
        tenant_id: uuid::Uuid,
        project_id: uuid::Uuid,
        signal_kind: Option<&str>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> anyhow::Result<Vec<StorageUsageDailySnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                project_id,
                signal_kind,
                day::text AS day,
                compressed_bytes,
                file_count,
                captured_at,
                false AS estimated_today
            FROM retention_storage_daily
            WHERE tenant_id = $1
              AND project_id = $2
              AND bucket_index = 0
              AND day < CURRENT_DATE
              AND ($3::text IS NULL OR signal_kind = $3)
              AND ($4::bigint IS NULL OR day >= (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
              AND ($5::bigint IS NULL OR day <= (to_timestamp($5::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
            ORDER BY day DESC, signal_kind
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
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
            let today_rows = derive_storage_usage_daily_rows(
                &self.client,
                tenant_id,
                project_id,
                signal_kind,
                today,
            )
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

fn storage_usage_daily_snapshot_from_row(row: sqlx::postgres::PgRow) -> StorageUsageDailySnapshot {
    StorageUsageDailySnapshot {
        tenant_id: row.get("tenant_id"),
        project_id: row.get("project_id"),
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
    tenant_id: uuid::Uuid,
    project_id: uuid::Uuid,
    signal_filter: Option<&str>,
    day: NaiveDate,
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    sqlx::query(
        r#"
        WITH params AS (
            SELECT $4::date AS snapshot_day,
                   ($4::date - INTERVAL '1 day')::date AS previous_day,
                   $5::bigint AS captured_at
        ),
        has_previous AS (
            SELECT EXISTS (
                SELECT 1
                FROM retention_storage_daily r, params p
                WHERE r.tenant_id = $1
                  AND r.project_id = $2
                  AND r.bucket_index = 0
                  AND r.day = p.previous_day
                  AND ($3::text IS NULL OR r.signal_kind = $3)
            ) AS value
        ),
        previous AS (
            SELECT r.tenant_id,
                   r.project_id,
                   r.signal_kind,
                   r.compressed_bytes,
                   r.file_count
            FROM retention_storage_daily r, params p
            WHERE r.tenant_id = $1
              AND r.project_id = $2
              AND r.bucket_index = 0
              AND r.day = p.previous_day
              AND ($3::text IS NULL OR r.signal_kind = $3)
        ),
        added AS (
            SELECT tenant_id,
                   project_id,
                   signal_kind,
                   compressed_bytes,
                   file_count
            FROM ingestion_daily i, params p
            WHERE i.tenant_id = $1
              AND i.project_id = $2
              AND i.day = p.snapshot_day
              AND ($3::text IS NULL OR i.signal_kind = $3)
        ),
        removed AS (
            SELECT tenant_id,
                   project_id,
                   signal_kind,
                   compressed_bytes,
                   file_count
            FROM storage_cleanup_daily c, params p
            WHERE c.tenant_id = $1
              AND c.project_id = $2
              AND c.day = p.snapshot_day
              AND ($3::text IS NULL OR c.signal_kind = $3)
        ),
        keys AS (
            SELECT tenant_id, project_id, signal_kind FROM previous
            UNION
            SELECT tenant_id, project_id, signal_kind FROM added
            UNION
            SELECT tenant_id, project_id, signal_kind FROM removed
        ),
        incremental AS (
            SELECT k.tenant_id,
                   k.project_id,
                   k.signal_kind,
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
            FROM keys k
            LEFT JOIN previous p USING (tenant_id, project_id, signal_kind)
            LEFT JOIN added a USING (tenant_id, project_id, signal_kind)
            LEFT JOIN removed r USING (tenant_id, project_id, signal_kind)
            WHERE (SELECT value FROM has_previous)
        ),
        bootstrap AS (
            SELECT tenant_id,
                   project_id,
                   signal_type AS signal_kind,
                   COALESCE(SUM(compressed_size), 0)::bigint AS compressed_bytes,
                   COUNT(*)::bigint AS file_count
            FROM file_list
            WHERE tenant_id = $1
              AND project_id = $2
              AND deleted = false
              AND ($3::text IS NULL OR signal_type = $3)
            GROUP BY tenant_id, project_id, signal_type
            HAVING NOT (SELECT value FROM has_previous)
        ),
        source AS (
            SELECT * FROM incremental
            UNION ALL
            SELECT * FROM bootstrap
        )
        SELECT s.tenant_id,
               s.project_id,
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
    .bind(tenant_id)
    .bind(project_id)
    .bind(signal_filter)
    .bind(day)
    .bind(chrono::Utc::now().timestamp_micros())
    .fetch_all(client.pool())
    .await
}
