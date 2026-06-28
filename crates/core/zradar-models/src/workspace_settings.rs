use crate::WorkspaceId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkspaceSettings {
    pub id: i64,
    pub workspace_id: WorkspaceId,
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
    /// When false, llm_input and llm_output are cleared before persisting.
    #[sqlx(default)]
    pub capture_llm_content_enabled: bool,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewWorkspaceSettings {
    pub workspace_id: WorkspaceId,
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
    pub capture_llm_content_enabled: bool,
}

impl NewWorkspaceSettings {
    pub fn defaults_for(workspace_id: WorkspaceId) -> Self {
        Self {
            workspace_id,
            traces_retention_days: 90,
            metrics_retention_days: 30,
            logs_retention_days: 30,
            max_ingestion_rate: None,
            file_push_interval_secs: 300,
            blocked: false,
            capture_llm_content_enabled: true,
        }
    }
}
