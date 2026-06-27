use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{NewAuditLog, NewProjectSettings, ProjectSettings};
use zradar_traits::{AuditLogRepository, SettingsRepository};

use crate::errors::{ControlError, Result};
use crate::http::{AuthContext, Capability};

pub struct SettingsState {
    pub repository: Arc<dyn SettingsRepository>,
    pub audit_log_repo: Option<Arc<dyn AuditLogRepository>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateProjectSettingsRequest {
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

pub async fn get_project_settings(
    State(state): State<Arc<SettingsState>>,
    auth: AuthContext,
    Path(project_id): Path<Uuid>,
) -> Result<Json<ProjectSettings>> {
    auth.require(Capability::ReadSettings)?;
    auth.enforce_path_project(project_id)?;

    let settings = match state.repository.get_settings(project_id).await? {
        Some(settings) => settings,
        None => {
            state
                .repository
                .upsert_settings(NewProjectSettings::defaults_for(project_id))
                .await?
        }
    };

    Ok(Json(settings))
}

pub async fn update_project_settings(
    State(state): State<Arc<SettingsState>>,
    auth: AuthContext,
    Path(project_id): Path<Uuid>,
    Json(body): Json<UpdateProjectSettingsRequest>,
) -> Result<Json<ProjectSettings>> {
    auth.require(Capability::WriteSettings)?;
    auth.enforce_path_project(project_id)?;
    validate_settings(&body)?;

    let settings = state
        .repository
        .upsert_settings(NewProjectSettings {
            project_id,
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
        let actor_tenant_id = auth.tenant_uuid().ok();
        let actor_project_id = auth.project_uuid().ok();
        audit_log_repo
            .create_audit_log(NewAuditLog {
                actor_tenant_id,
                actor_project_id,
                org_id: actor_tenant_id,
                project_id: Some(project_id),
                action: "project_settings.update".to_string(),
                resource_type: "project_settings".to_string(),
                resource_id: project_id.to_string(),
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

fn validate_settings(body: &UpdateProjectSettingsRequest) -> Result<()> {
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
