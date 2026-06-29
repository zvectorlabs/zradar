//! Settings administration service trait.
//!
//! Abstracts workspace settings CRUD using the existing model types
//! (`WorkspaceSettings`, `NewWorkspaceSettings`).

use async_trait::async_trait;
use zradar_models::{NewWorkspaceSettings, WorkspaceId, WorkspaceSettings};

use crate::errors::ServiceError;

/// Workspace settings administration service trait.
#[async_trait]
pub trait SettingsAdminService: Send + Sync {
    /// Get the current settings for a workspace, or `None` if not yet configured.
    async fn get_settings(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceSettings>, ServiceError>;

    /// Create or update workspace settings.
    async fn upsert_settings(
        &self,
        settings: NewWorkspaceSettings,
    ) -> Result<WorkspaceSettings, ServiceError>;
}
