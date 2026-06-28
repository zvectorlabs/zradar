use async_trait::async_trait;
use zradar_models::WorkspaceId;
use zradar_models::{NewWorkspaceSettings, WorkspaceSettings};

#[async_trait]
pub trait SettingsRepository: Send + Sync {
    async fn get_settings(
        &self,
        workspace_id: WorkspaceId,
    ) -> anyhow::Result<Option<WorkspaceSettings>>;
    async fn upsert_settings(
        &self,
        settings: NewWorkspaceSettings,
    ) -> anyhow::Result<WorkspaceSettings>;
    async fn list_all_settings(&self) -> anyhow::Result<Vec<WorkspaceSettings>>;
}
