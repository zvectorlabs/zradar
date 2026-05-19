use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use std::sync::Arc;
use zradar_models::{AuditLog, NewAuditLog};
use zradar_traits::{AuditLogFilters, AuditLogPage, AuditLogRepository};

pub struct PostgresAuditLogRepository {
    client: Arc<PostgresClient>,
}

impl PostgresAuditLogRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AuditLogRepository for PostgresAuditLogRepository {
    async fn create_audit_log(&self, log: NewAuditLog) -> anyhow::Result<AuditLog> {
        let now = chrono::Utc::now().timestamp_micros();
        let saved = sqlx::query_as::<_, AuditLog>(
            r#"
            INSERT INTO audit_logs (
                actor_tenant_id, actor_project_id, org_id, project_id, action,
                resource_type, resource_id, metadata, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING
                id, actor_tenant_id, actor_project_id, org_id, project_id, action,
                resource_type, resource_id, metadata, created_at
            "#,
        )
        .bind(log.actor_tenant_id)
        .bind(log.actor_project_id)
        .bind(log.org_id)
        .bind(log.project_id)
        .bind(log.action)
        .bind(log.resource_type)
        .bind(log.resource_id)
        .bind(log.metadata)
        .bind(now)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to create audit log")?;

        Ok(saved)
    }

    async fn list_audit_logs(&self, filters: AuditLogFilters) -> anyhow::Result<AuditLogPage> {
        let limit = filters.limit.unwrap_or(100).clamp(1, 500);
        let offset = filters.offset.unwrap_or(0);

        let items = sqlx::query_as::<_, AuditLog>(
            r#"
            SELECT
                id, actor_tenant_id, actor_project_id, org_id, project_id, action,
                resource_type, resource_id, metadata, created_at
            FROM audit_logs
            WHERE ($1::uuid IS NULL OR org_id = $1)
              AND ($2::uuid IS NULL OR project_id = $2)
              AND ($3::text IS NULL OR action = $3)
              AND ($4::text IS NULL OR resource_type = $4)
              AND ($5::text IS NULL OR resource_id = $5)
              AND ($6::bigint IS NULL OR created_at >= $6)
              AND ($7::bigint IS NULL OR created_at <= $7)
            ORDER BY created_at DESC, id DESC
            LIMIT $8 OFFSET $9
            "#,
        )
        .bind(filters.org_id)
        .bind(filters.project_id)
        .bind(filters.action.clone())
        .bind(filters.resource_type.clone())
        .bind(filters.resource_id.clone())
        .bind(filters.start_created_at)
        .bind(filters.end_created_at)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.client.pool())
        .await
        .context("Failed to list audit logs")?;

        let total: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*)
            FROM audit_logs
            WHERE ($1::uuid IS NULL OR org_id = $1)
              AND ($2::uuid IS NULL OR project_id = $2)
              AND ($3::text IS NULL OR action = $3)
              AND ($4::text IS NULL OR resource_type = $4)
              AND ($5::text IS NULL OR resource_id = $5)
              AND ($6::bigint IS NULL OR created_at >= $6)
              AND ($7::bigint IS NULL OR created_at <= $7)
            "#,
        )
        .bind(filters.org_id)
        .bind(filters.project_id)
        .bind(filters.action)
        .bind(filters.resource_type)
        .bind(filters.resource_id)
        .bind(filters.start_created_at)
        .bind(filters.end_created_at)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to count audit logs")?;

        Ok(AuditLogPage {
            items,
            total: total.0,
            limit,
            offset,
        })
    }
}
