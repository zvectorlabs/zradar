//! gRPC handler for the `SettingsService` RPC (workspace settings).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_models::{NewAuditLog, NewWorkspaceSettings};
use zradar_traits::{AdminAuthorizer, Capability};

use crate::errors::ControlError;
use crate::settings::handlers::SettingsState;

use super::admin_proto::settings_service_server::SettingsService as SettingsServiceRpc;
use super::admin_proto::*;
use super::auth::authorize_admin;
use super::conversions::{map_anyhow_error, map_control_error, workspace_settings_to_proto};

/// Tonic handler that delegates to [`SettingsState`].
pub struct SettingsHandler {
    state: Arc<SettingsState>,
    auth: Arc<dyn AdminAuthorizer>,
}

impl SettingsHandler {
    pub fn new(state: Arc<SettingsState>, auth: Arc<dyn AdminAuthorizer>) -> Self {
        Self { state, auth }
    }
}

#[tonic::async_trait]
impl SettingsServiceRpc for SettingsHandler {
    async fn get_workspace_settings(
        &self,
        request: Request<GetWorkspaceSettingsRequest>,
    ) -> Result<Response<GetWorkspaceSettingsResponse>, Status> {
        let (_req, auth) = authorize_admin(&self.auth, request, Capability::ReadSettings).await?;
        let workspace_id = auth.workspace_id();

        let settings = match self
            .state
            .repository
            .get_settings(workspace_id)
            .await
            .map_err(map_anyhow_error)?
        {
            Some(settings) => settings,
            None => self
                .state
                .repository
                .upsert_settings(NewWorkspaceSettings::defaults_for(workspace_id))
                .await
                .map_err(map_anyhow_error)?,
        };

        Ok(Response::new(GetWorkspaceSettingsResponse {
            settings: Some(workspace_settings_to_proto(&settings)),
        }))
    }

    async fn update_workspace_settings(
        &self,
        request: Request<UpdateWorkspaceSettingsRequest>,
    ) -> Result<Response<UpdateWorkspaceSettingsResponse>, Status> {
        let (req, auth) = authorize_admin(&self.auth, request, Capability::WriteSettings).await?;
        let workspace_id = auth.workspace_id();

        validate_settings(&req)?;

        let settings = self
            .state
            .repository
            .upsert_settings(NewWorkspaceSettings {
                workspace_id,
                traces_retention_days: req.traces_retention_days,
                metrics_retention_days: req.metrics_retention_days,
                logs_retention_days: req.logs_retention_days,
                max_ingestion_rate: req.max_ingestion_rate,
                file_push_interval_secs: req.file_push_interval_secs,
                blocked: req.blocked,
                capture_llm_content_enabled: req.capture_llm_content_enabled,
            })
            .await
            .map_err(map_anyhow_error)?;

        if let Some(audit_log_repo) = &self.state.audit_log_repo {
            audit_log_repo
                .create_audit_log(NewAuditLog {
                    actor_workspace_id: Some(workspace_id.into_inner()),
                    resource_workspace_id: Some(workspace_id.into_inner()),
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
                .await
                .map_err(map_anyhow_error)?;
        }

        Ok(Response::new(UpdateWorkspaceSettingsResponse {
            settings: Some(workspace_settings_to_proto(&settings)),
        }))
    }
}

fn validate_settings(body: &UpdateWorkspaceSettingsRequest) -> Result<(), Status> {
    if body.traces_retention_days < 0 {
        return Err(map_control_error(ControlError::InvalidInput(
            "traces_retention_days must be non-negative".to_string(),
        )));
    }
    if body.metrics_retention_days < 0 {
        return Err(map_control_error(ControlError::InvalidInput(
            "metrics_retention_days must be non-negative".to_string(),
        )));
    }
    if body.logs_retention_days < 0 {
        return Err(map_control_error(ControlError::InvalidInput(
            "logs_retention_days must be non-negative".to_string(),
        )));
    }
    if let Some(rate) = body.max_ingestion_rate
        && rate < 0
    {
        return Err(map_control_error(ControlError::InvalidInput(
            "max_ingestion_rate must be non-negative".to_string(),
        )));
    }
    if body.file_push_interval_secs <= 0 {
        return Err(map_control_error(ControlError::InvalidInput(
            "file_push_interval_secs must be positive".to_string(),
        )));
    }

    Ok(())
}
