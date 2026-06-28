use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use std::sync::Arc;
use zradar_models::WorkspaceId;
use zradar_models::{NewWorkspaceSettings, WorkspaceSettings};
use zradar_traits::SettingsRepository;

pub struct PostgresSettingsRepository {
    client: Arc<PostgresClient>,
}

impl PostgresSettingsRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SettingsRepository for PostgresSettingsRepository {
    async fn get_settings(
        &self,
        workspace_id: WorkspaceId,
    ) -> anyhow::Result<Option<WorkspaceSettings>> {
        let settings = sqlx::query_as::<_, WorkspaceSettings>(
            r#"
            SELECT
                id, workspace_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, capture_llm_content_enabled, updated_at
            FROM workspace_settings
            WHERE workspace_id = $1
            "#,
        )
        .bind(workspace_id.into_inner())
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to get workspace settings")?;

        Ok(settings)
    }

    async fn upsert_settings(
        &self,
        settings: NewWorkspaceSettings,
    ) -> anyhow::Result<WorkspaceSettings> {
        let now = chrono::Utc::now().timestamp_micros();
        let saved = sqlx::query_as::<_, WorkspaceSettings>(
            r#"
            INSERT INTO workspace_settings (
                workspace_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, capture_llm_content_enabled, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
            ON CONFLICT (workspace_id)
            DO UPDATE SET
                traces_retention_days = EXCLUDED.traces_retention_days,
                metrics_retention_days = EXCLUDED.metrics_retention_days,
                logs_retention_days = EXCLUDED.logs_retention_days,
                max_ingestion_rate = EXCLUDED.max_ingestion_rate,
                file_push_interval_secs = EXCLUDED.file_push_interval_secs,
                blocked = EXCLUDED.blocked,
                capture_llm_content_enabled = EXCLUDED.capture_llm_content_enabled,
                updated_at = EXCLUDED.updated_at
            RETURNING
                id, workspace_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, capture_llm_content_enabled, updated_at
            "#,
        )
        .bind(settings.workspace_id.into_inner())
        .bind(settings.traces_retention_days)
        .bind(settings.metrics_retention_days)
        .bind(settings.logs_retention_days)
        .bind(settings.max_ingestion_rate)
        .bind(settings.file_push_interval_secs)
        .bind(settings.blocked)
        .bind(settings.capture_llm_content_enabled)
        .bind(now)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to upsert workspace settings")?;

        Ok(saved)
    }

    async fn list_all_settings(&self) -> anyhow::Result<Vec<WorkspaceSettings>> {
        let settings = sqlx::query_as::<_, WorkspaceSettings>(
            r#"
            SELECT
                id, workspace_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, capture_llm_content_enabled, updated_at
            FROM workspace_settings
            "#,
        )
        .fetch_all(self.client.pool())
        .await
        .context("Failed to list all workspace settings")?;

        Ok(settings)
    }
}
