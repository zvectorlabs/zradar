//! gRPC handler for the `RetentionService` RPC (retention management).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_retention::WorkspaceRetentionConfig;
use zradar_traits::{AdminAuthorizer, Capability};

use crate::retention::handlers::RetentionState;

use super::admin_proto::retention_service_server::RetentionService as RetentionServiceRpc;
use super::admin_proto::*;
use super::auth::authorize_admin;
use super::conversions::{map_anyhow_error, retention_run_stats_to_proto};

/// Tonic handler that delegates to [`RetentionState`].
pub struct RetentionHandler {
    state: Arc<RetentionState>,
    auth: Arc<dyn AdminAuthorizer>,
}

impl RetentionHandler {
    pub fn new(state: Arc<RetentionState>, auth: Arc<dyn AdminAuthorizer>) -> Self {
        Self { state, auth }
    }
}

#[tonic::async_trait]
impl RetentionServiceRpc for RetentionHandler {
    async fn run_cleanup(
        &self,
        request: Request<RunCleanupRequest>,
    ) -> Result<Response<RunCleanupResponse>, Status> {
        let (req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();
        let uuid = workspace_id.into_inner();

        if let Some(days) = req.retention_days {
            self.state.config_store.upsert(WorkspaceRetentionConfig {
                workspace_id,
                retention_days: days,
            });
        }

        let mark_stats = self
            .state
            .cleanup_job
            .run_now_scoped(Some(uuid))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let reclaim_stats = self
            .state
            .file_reclaimer
            .run_now_scoped(Some(uuid))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let stats = mark_stats.with_reclaim(&reclaim_stats);

        Ok(Response::new(RunCleanupResponse {
            stats: Some(retention_run_stats_to_proto(&stats)),
        }))
    }

    async fn get_workspace_retention(
        &self,
        request: Request<GetWorkspaceRetentionRequest>,
    ) -> Result<Response<GetWorkspaceRetentionResponse>, Status> {
        let (_req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();

        match self
            .state
            .settings_repo
            .get_settings(workspace_id)
            .await
            .map_err(map_anyhow_error)?
        {
            Some(settings) => {
                let retention_days = settings.traces_retention_days as u32;
                Ok(Response::new(GetWorkspaceRetentionResponse {
                    workspace_id: workspace_id.to_string(),
                    retention_days,
                    inherited: false,
                    workspace_default_days: retention_days,
                }))
            }
            None => {
                let retention_days = self.state.config_store.get_effective_days(workspace_id);
                Ok(Response::new(GetWorkspaceRetentionResponse {
                    workspace_id: workspace_id.to_string(),
                    retention_days,
                    inherited: true,
                    workspace_default_days: retention_days,
                }))
            }
        }
    }

    async fn set_workspace_retention(
        &self,
        request: Request<SetWorkspaceRetentionRequest>,
    ) -> Result<Response<SetWorkspaceRetentionResponse>, Status> {
        let (req, auth) = authorize_admin(&self.auth, request, Capability::WriteSettings).await?;
        let workspace_id = auth.workspace_id();

        let settings = match self
            .state
            .settings_repo
            .get_settings(workspace_id)
            .await
            .map_err(map_anyhow_error)?
        {
            Some(s) => zradar_models::NewWorkspaceSettings {
                workspace_id: s.workspace_id,
                traces_retention_days: req.retention_days as i32,
                metrics_retention_days: req.retention_days as i32,
                logs_retention_days: req.retention_days as i32,
                max_ingestion_rate: s.max_ingestion_rate,
                file_push_interval_secs: s.file_push_interval_secs,
                blocked: s.blocked,
                capture_llm_content_enabled: s.capture_llm_content_enabled,
            },
            None => {
                let mut s = zradar_models::NewWorkspaceSettings::defaults_for(workspace_id);
                s.traces_retention_days = req.retention_days as i32;
                s.metrics_retention_days = req.retention_days as i32;
                s.logs_retention_days = req.retention_days as i32;
                s
            }
        };

        let saved = self
            .state
            .settings_repo
            .upsert_settings(settings)
            .await
            .map_err(map_anyhow_error)?;

        self.state.config_store.upsert(WorkspaceRetentionConfig {
            workspace_id,
            retention_days: req.retention_days,
        });

        Ok(Response::new(SetWorkspaceRetentionResponse {
            workspace_id: workspace_id.to_string(),
            retention_days: req.retention_days,
            inherited: false,
            workspace_default_days: saved.traces_retention_days as u32,
        }))
    }
}
