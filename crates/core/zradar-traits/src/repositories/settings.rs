use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::{NewProjectSettings, ProjectSettings};

#[async_trait]
pub trait SettingsRepository: Send + Sync {
    async fn get_settings(&self, project_id: Uuid) -> anyhow::Result<Option<ProjectSettings>>;
    async fn upsert_settings(
        &self,
        settings: NewProjectSettings,
    ) -> anyhow::Result<ProjectSettings>;
}
