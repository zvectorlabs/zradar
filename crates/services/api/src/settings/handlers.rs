use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{NewAuditLog, NewWorkspaceSettings, WorkspaceSettings};
use zradar_traits::{AuditLogRepository, SettingsRepository};

use crate::errors::{ControlError, Result};
use crate::http::{AuthContext, Capability};

pub struct SettingsState {
    pub repository: Arc<dyn SettingsRepository>,
    pub audit_log_repo: Option<Arc<dyn AuditLogRepository>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWorkspaceSettingsRequest {
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
    #[serde(default = "default_capture_llm_content")]
    pub capture_llm_content_enabled: bool,
}

fn default_capture_llm_content() -> bool {
    true
}

pub async fn get_workspace_settings(
    State(state): State<Arc<SettingsState>>,
    auth: AuthContext,
    Path(workspace_id): Path<Uuid>,
) -> Result<Json<WorkspaceSettings>> {
    auth.require(Capability::ReadSettings)?;
    auth.enforce_path_workspace(workspace_id)?;

    let settings = match state.repository.get_settings(workspace_id.into()).await? {
        Some(settings) => settings,
        None => {
            state
                .repository
                .upsert_settings(NewWorkspaceSettings::defaults_for(workspace_id.into()))
                .await?
        }
    };

    Ok(Json(settings))
}

pub async fn update_workspace_settings(
    State(state): State<Arc<SettingsState>>,
    auth: AuthContext,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<UpdateWorkspaceSettingsRequest>,
) -> Result<Json<WorkspaceSettings>> {
    auth.require(Capability::WriteSettings)?;
    auth.enforce_path_workspace(workspace_id)?;
    validate_settings(&body)?;

    let settings = state
        .repository
        .upsert_settings(NewWorkspaceSettings {
            workspace_id: workspace_id.into(),
            traces_retention_days: body.traces_retention_days,
            metrics_retention_days: body.metrics_retention_days,
            logs_retention_days: body.logs_retention_days,
            max_ingestion_rate: body.max_ingestion_rate,
            file_push_interval_secs: body.file_push_interval_secs,
            blocked: body.blocked,
            capture_llm_content_enabled: body.capture_llm_content_enabled,
        })
        .await?;

    if let Some(audit_log_repo) = &state.audit_log_repo {
        let actor_workspace_id = auth.workspace_uuid().ok();
        audit_log_repo
            .create_audit_log(NewAuditLog {
                actor_workspace_id,
                resource_workspace_id: Some(workspace_id),
                action: "workspace_settings.update".to_string(),
                resource_type: "workspace_settings".to_string(),
                resource_id: workspace_id.to_string(),
                metadata: serde_json::json!({
                    "traces_retention_days": settings.traces_retention_days,
                    "metrics_retention_days": settings.metrics_retention_days,
                    "logs_retention_days": settings.logs_retention_days,
                    "max_ingestion_rate": settings.max_ingestion_rate,
                    "file_push_interval_secs": settings.file_push_interval_secs,
                    "blocked": settings.blocked,
                    "capture_llm_content_enabled": settings.capture_llm_content_enabled,
                }),
            })
            .await?;
    }

    Ok(Json(settings))
}

fn validate_settings(body: &UpdateWorkspaceSettingsRequest) -> Result<()> {
    if body.traces_retention_days < 0 {
        return Err(ControlError::InvalidInput(
            "traces_retention_days must be non-negative".to_string(),
        ));
    }
    if body.metrics_retention_days < 0 {
        return Err(ControlError::InvalidInput(
            "metrics_retention_days must be non-negative".to_string(),
        ));
    }
    if body.logs_retention_days < 0 {
        return Err(ControlError::InvalidInput(
            "logs_retention_days must be non-negative".to_string(),
        ));
    }
    if let Some(rate) = body.max_ingestion_rate
        && rate < 0
    {
        return Err(ControlError::InvalidInput(
            "max_ingestion_rate must be non-negative".to_string(),
        ));
    }
    if body.file_push_interval_secs <= 0 {
        return Err(ControlError::InvalidInput(
            "file_push_interval_secs must be positive".to_string(),
        ));
    }

    Ok(())
}
