use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ProjectSettings {
    pub id: i64,
    pub project_id: Uuid,
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProjectSettings {
    pub project_id: Uuid,
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
}

impl NewProjectSettings {
    pub fn defaults_for(project_id: Uuid) -> Self {
        Self {
            project_id,
            traces_retention_days: 90,
            metrics_retention_days: 30,
            logs_retention_days: 30,
            max_ingestion_rate: None,
            file_push_interval_secs: 300,
            blocked: false,
        }
    }
}
