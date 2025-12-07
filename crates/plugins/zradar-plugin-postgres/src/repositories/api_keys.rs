//! PostgreSQL API key repository implementation

use async_trait::async_trait;
use chrono::Utc;
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::PostgresClient;
use zradar_traits::{ApiKey, ApiKeyRepository, CreateApiKeyRequest};

pub struct PostgresApiKeyRepository {
    client: Arc<PostgresClient>,
}

impl PostgresApiKeyRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ApiKeyRepository for PostgresApiKeyRepository {
    async fn create_key(
        &self,
        org_id: Uuid,
        project_id: Uuid,
        key_hash: String,
        key_prefix: String,
        req: CreateApiKeyRequest,
        created_by: Uuid,
    ) -> anyhow::Result<ApiKey> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        // Calculate expires_at from expires_in_days
        let expires_at = req
            .expires_in_days
            .map(|days| now + chrono::Duration::days(days as i64));

        let permissions = req
            .permissions
            .unwrap_or_else(|| vec!["write:traces".to_string(), "write:metrics".to_string()]);

        let row = sqlx::query(
            r#"
            INSERT INTO api_keys (
                id, organization_id, project_id, name, description, key_hash, key_prefix,
                permissions, is_active, expires_at, last_used_at, rate_limit_per_minute,
                created_by, created_at, updated_at, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            RETURNING id, organization_id, project_id, name, description, key_hash, key_prefix,
                      permissions, ip_whitelist, rate_limit_per_minute, is_active,
                      created_by, created_at, updated_at, last_used_at, expires_at, metadata
            "#,
        )
        .bind(id)
        .bind(org_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&key_hash)
        .bind(&key_prefix)
        .bind(&permissions)
        .bind(true) // is_active
        .bind(expires_at)
        .bind(None::<chrono::DateTime<Utc>>) // last_used_at
        .bind(req.rate_limit_per_minute)
        .bind(created_by)
        .bind(now)
        .bind(now)
        .bind(serde_json::json!({}))
        .fetch_one(self.client.pool())
        .await?;

        Ok(ApiKey {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            project_id: row.get("project_id"),
            key_hash: row.get("key_hash"),
            key_prefix: row.get("key_prefix"),
            name: row.get("name"),
            description: row.get("description"),
            permissions: row.get("permissions"),
            ip_whitelist: row.get("ip_whitelist"),
            rate_limit_per_minute: row.get("rate_limit_per_minute"),
            is_active: row.get("is_active"),
            created_by: row.get("created_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            last_used_at: row.get("last_used_at"),
            expires_at: row.get("expires_at"),
            metadata: row.get("metadata"),
        })
    }

    async fn get_key(&self, id: Uuid) -> anyhow::Result<Option<ApiKey>> {
        let row = sqlx::query("SELECT * FROM api_keys WHERE id = $1")
            .bind(id)
            .fetch_optional(self.client.pool())
            .await?;

        Ok(row.map(|r| ApiKey {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            project_id: r.get("project_id"),
            key_hash: r.get("key_hash"),
            key_prefix: r.get("key_prefix"),
            name: r.get("name"),
            description: r.get("description"),
            permissions: r.get("permissions"),
            ip_whitelist: r.get("ip_whitelist"),
            rate_limit_per_minute: r.get("rate_limit_per_minute"),
            is_active: r.get("is_active"),
            created_by: r.get("created_by"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
            last_used_at: r.get("last_used_at"),
            expires_at: r.get("expires_at"),
            metadata: r.get("metadata"),
        }))
    }

    async fn get_key_by_hash(&self, hash: &str) -> anyhow::Result<Option<ApiKey>> {
        let row = sqlx::query("SELECT * FROM api_keys WHERE key_hash = $1 AND is_active = true")
            .bind(hash)
            .fetch_optional(self.client.pool())
            .await?;

        Ok(row.map(|r| ApiKey {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            project_id: r.get("project_id"),
            key_hash: r.get("key_hash"),
            key_prefix: r.get("key_prefix"),
            name: r.get("name"),
            description: r.get("description"),
            permissions: r.get("permissions"),
            ip_whitelist: r.get("ip_whitelist"),
            rate_limit_per_minute: r.get("rate_limit_per_minute"),
            is_active: r.get("is_active"),
            created_by: r.get("created_by"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
            last_used_at: r.get("last_used_at"),
            expires_at: r.get("expires_at"),
            metadata: r.get("metadata"),
        }))
    }

    async fn list_keys(&self, org_id: Uuid, project_id: Uuid) -> anyhow::Result<Vec<ApiKey>> {
        let rows = sqlx::query(
            "SELECT * FROM api_keys 
             WHERE organization_id = $1 AND project_id = $2 
             ORDER BY created_at DESC",
        )
        .bind(org_id)
        .bind(project_id)
        .fetch_all(self.client.pool())
        .await?;

        let keys = rows
            .into_iter()
            .map(|r| ApiKey {
                id: r.get("id"),
                organization_id: r.get("organization_id"),
                project_id: r.get("project_id"),
                key_hash: r.get("key_hash"),
                key_prefix: r.get("key_prefix"),
                name: r.get("name"),
                description: r.get("description"),
                permissions: r.get("permissions"),
                ip_whitelist: r.get("ip_whitelist"),
                rate_limit_per_minute: r.get("rate_limit_per_minute"),
                is_active: r.get("is_active"),
                created_by: r.get("created_by"),
                created_at: r.get("created_at"),
                updated_at: r.get("updated_at"),
                last_used_at: r.get("last_used_at"),
                expires_at: r.get("expires_at"),
                metadata: r.get("metadata"),
            })
            .collect();

        Ok(keys)
    }

    async fn revoke_key(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("UPDATE api_keys SET is_active = false, updated_at = $1 WHERE id = $2")
            .bind(Utc::now())
            .bind(id)
            .execute(self.client.pool())
            .await?;

        Ok(())
    }

    async fn delete_key(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM api_keys WHERE id = $1")
            .bind(id)
            .execute(self.client.pool())
            .await?;

        Ok(())
    }

    async fn update_last_used(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("UPDATE api_keys SET last_used_at = $1 WHERE id = $2")
            .bind(Utc::now())
            .bind(id)
            .execute(self.client.pool())
            .await?;

        Ok(())
    }
}
