use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{NewProjectSettings, ProjectSettings};
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
    async fn get_settings(&self, project_id: Uuid) -> anyhow::Result<Option<ProjectSettings>> {
        let settings = sqlx::query_as::<_, ProjectSettings>(
            r#"
            SELECT
                id, project_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, updated_at
            FROM project_settings
            WHERE project_id = $1
            "#,
        )
        .bind(project_id)
        .fetch_optional(self.client.pool())
        .await
        .context("Failed to get project settings")?;

        Ok(settings)
    }

    async fn upsert_settings(
        &self,
        settings: NewProjectSettings,
    ) -> anyhow::Result<ProjectSettings> {
        let now = chrono::Utc::now().timestamp_micros();
        let saved = sqlx::query_as::<_, ProjectSettings>(
            r#"
            INSERT INTO project_settings (
                project_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (project_id)
            DO UPDATE SET
                traces_retention_days = EXCLUDED.traces_retention_days,
                metrics_retention_days = EXCLUDED.metrics_retention_days,
                logs_retention_days = EXCLUDED.logs_retention_days,
                max_ingestion_rate = EXCLUDED.max_ingestion_rate,
                file_push_interval_secs = EXCLUDED.file_push_interval_secs,
                blocked = EXCLUDED.blocked,
                updated_at = EXCLUDED.updated_at
            RETURNING
                id, project_id, traces_retention_days, metrics_retention_days,
                logs_retention_days, max_ingestion_rate, file_push_interval_secs,
                blocked, updated_at
            "#,
        )
        .bind(settings.project_id)
        .bind(settings.traces_retention_days)
        .bind(settings.metrics_retention_days)
        .bind(settings.logs_retention_days)
        .bind(settings.max_ingestion_rate)
        .bind(settings.file_push_interval_secs)
        .bind(settings.blocked)
        .bind(now)
        .fetch_one(self.client.pool())
        .await
        .context("Failed to upsert project settings")?;

        Ok(saved)
    }
}
