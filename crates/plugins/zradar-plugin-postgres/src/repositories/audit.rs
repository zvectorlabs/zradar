//! PostgreSQL audit logger implementation

use crate::client::PostgresClient;
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;
use zradar_traits::{AuditEvent, AuditLog, AuditLogger};

pub struct PostgresAuditLogger {
    client: Arc<PostgresClient>,
}

impl PostgresAuditLogger {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AuditLogger for PostgresAuditLogger {
    async fn log(&self, event: AuditEvent) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO audit_logs 
                (organization_id, user_id, actor_type, actor_id, actor_ip, 
                 action, resource_type, resource_id, status, details)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(event.organization_id)
        .bind(event.user_id)
        .bind(&event.actor_type)
        .bind(event.actor_id)
        .bind(&event.actor_ip)
        .bind(&event.action)
        .bind(&event.resource_type)
        .bind(event.resource_id)
        .bind(event.status.as_str())
        .bind(&event.details)
        .execute(self.client.pool())
        .await?;

        Ok(())
    }

    async fn get_logs(
        &self,
        org_id: Option<Uuid>,
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<AuditLog>> {
        let limit = limit.unwrap_or(100);

        let rows = if let Some(org_id) = org_id {
            sqlx::query_as::<_, AuditLogRow>(
                r#"
                SELECT id, organization_id, user_id, actor_type, actor_id, actor_ip,
                       action, resource_type, resource_id, status, details, created_at
                FROM audit_logs
                WHERE organization_id = $1
                ORDER BY created_at DESC
                LIMIT $2
                "#,
            )
            .bind(org_id)
            .bind(limit)
            .fetch_all(self.client.pool())
            .await?
        } else {
            sqlx::query_as::<_, AuditLogRow>(
                r#"
                SELECT id, organization_id, user_id, actor_type, actor_id, actor_ip,
                       action, resource_type, resource_id, status, details, created_at
                FROM audit_logs
                ORDER BY created_at DESC
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(self.client.pool())
            .await?
        };

        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AuditLogRow {
    id: Uuid,
    organization_id: Option<Uuid>,
    user_id: Option<Uuid>,
    actor_type: Option<String>,
    actor_id: Option<Uuid>,
    actor_ip: Option<String>,
    action: String,
    resource_type: Option<String>,
    resource_id: Option<Uuid>,
    status: String,
    details: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<AuditLogRow> for AuditLog {
    fn from(row: AuditLogRow) -> Self {
        AuditLog {
            id: row.id,
            organization_id: row.organization_id,
            user_id: row.user_id,
            actor_type: row.actor_type,
            actor_id: row.actor_id,
            actor_ip: row.actor_ip,
            action: row.action,
            resource_type: row.resource_type,
            resource_id: row.resource_id,
            status: row.status,
            details: row.details,
            created_at: row.created_at,
        }
    }
}
