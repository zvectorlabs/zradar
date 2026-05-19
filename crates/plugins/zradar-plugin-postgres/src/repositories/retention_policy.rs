use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{NewRetentionPolicy, RetentionPolicy};
use zradar_traits::RetentionPolicyRepository;

pub struct PostgresRetentionPolicyRepository {
    client: Arc<PostgresClient>,
}

impl PostgresRetentionPolicyRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RetentionPolicyRepository for PostgresRetentionPolicyRepository {
    async fn get_policy(&self, org_id: Uuid) -> anyhow::Result<Option<RetentionPolicy>> {
        let policy = sqlx::query_as::<_, RetentionPolicy>(
            r#"
            SELECT id, org_id, default_days, project_overrides, updated_at
            FROM retention_policies
            WHERE org_id = $1
            "#,
        )
        .bind(org_id)
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to get retention policy")?;

        Ok(policy)
    }

    async fn list_policies(&self) -> anyhow::Result<Vec<RetentionPolicy>> {
        let policies = sqlx::query_as::<_, RetentionPolicy>(
            r#"
            SELECT id, org_id, default_days, project_overrides, updated_at
            FROM retention_policies
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(self.client.pool())
        .await
        .context("Failed to list retention policies")?;

        Ok(policies)
    }

    async fn upsert_policy(&self, policy: NewRetentionPolicy) -> anyhow::Result<RetentionPolicy> {
        let now = chrono::Utc::now().timestamp_micros();
        let project_overrides = serde_json::to_value(policy.project_overrides)
            .context("Failed to serialize project retention overrides")?;
        let saved = sqlx::query_as::<_, RetentionPolicy>(
            r#"
            INSERT INTO retention_policies (
                org_id, default_days, project_overrides, updated_at
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (org_id)
            DO UPDATE SET
                default_days = EXCLUDED.default_days,
                project_overrides = EXCLUDED.project_overrides,
                updated_at = EXCLUDED.updated_at
            RETURNING id, org_id, default_days, project_overrides, updated_at
            "#,
        )
        .bind(policy.org_id)
        .bind(policy.default_days)
        .bind(project_overrides)
        .bind(now)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to upsert retention policy")?;

        Ok(saved)
    }
}
