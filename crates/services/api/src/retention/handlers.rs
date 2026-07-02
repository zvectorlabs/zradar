//! Retention admin handlers.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::WorkspaceId;

use zradar_retention::{
    CleanupJob, FileReclaimer, RetentionConfigStore, RetentionRunStats, WorkspaceRetentionConfig,
};
use zradar_traits::{AuditLogRepository, SettingsRepository};

use crate::http::{AuthContext, Capability};

/// Shared state for retention handlers.
pub struct RetentionState {
    pub cleanup_job: Arc<CleanupJob>,
    pub file_reclaimer: Arc<FileReclaimer>,
    pub config_store: Arc<RetentionConfigStore>,
    pub settings_repo: Arc<dyn SettingsRepository>,
    pub audit_log_repo: Option<Arc<dyn AuditLogRepository>>,
}

/// Query parameters for `POST /api/v1/admin/retention/run`.
#[derive(Debug, Deserialize)]
pub struct RunCleanupParams {
    /// Override retention in days for this run only.
    /// When set, all files older than this many days are deleted regardless
    /// of the stored per-org config.  0 means delete everything.
    pub retention_days: Option<u32>,
    /// If provided, restrict cleanup to this workspace.
    pub workspace_id: Option<Uuid>,
}

/// Response body for a cleanup run.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RunCleanupResponse {
    pub stats: RetentionRunStats,
}

/// `POST /api/v1/admin/retention/run`
///
/// Trigger a retention cleanup cycle synchronously.
/// Returns cleanup statistics.
#[utoipa::path(
    post,
    path = "/api/v1/admin/retention/run",
    params(
        ("retention_days" = Option<u32>, Query, description = "Override retention days (0 = delete all)"),
        ("workspace_id" = Option<Uuid>, Query, description = "Restrict to a specific workspace"),
    ),
    responses(
        (status = 200, description = "Cleanup completed", body = RunCleanupResponse),
        (status = 500, description = "Cleanup failed"),
    ),
    tag = "retention"
)]
pub async fn run_cleanup(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Query(params): Query<RunCleanupParams>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }

    let workspace_id = match auth.workspace_or_standalone_override(params.workspace_id) {
        Ok(workspace_id) => workspace_id,
        Err(e) => return e.into_response(),
    };

    if let Some(days) = params.retention_days {
        state.config_store.upsert(WorkspaceRetentionConfig {
            workspace_id: workspace_id.into(),
            retention_days: days,
        });
    }

    let mark_stats = match state.cleanup_job.run_now_scoped(Some(workspace_id)).await {
        Ok(stats) => stats,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    match state
        .file_reclaimer
        .run_now_scoped(Some(workspace_id))
        .await
    {
        Ok(reclaim_stats) => (
            StatusCode::OK,
            Json(RunCleanupResponse {
                stats: mark_stats.with_reclaim(&reclaim_stats),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct SetRetentionConfigRequest {
    #[serde(default)]
    pub workspace_id: Option<Uuid>,
    pub default_days: u32,
    #[serde(default)]
    pub workspace_overrides: std::collections::HashMap<Uuid, u32>,
}

pub async fn set_retention_config() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({ "error": "Not implemented" })),
    )
        .into_response()
}

pub async fn get_retention_config() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({ "error": "Not implemented" })),
    )
        .into_response()
}

pub async fn list_retention_configs() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({ "error": "Not implemented" })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct SetWorkspaceRetentionRequest {
    pub retention_days: u32,
}

#[derive(Debug, Serialize)]
pub struct WorkspaceRetentionResponse {
    pub workspace_id: WorkspaceId,
    pub retention_days: u32,
    pub inherited: bool,
    pub workspace_default_days: u32,
}

pub async fn get_workspace_retention(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    let auth_workspace_id = match auth.workspace_uuid() {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    if auth_workspace_id != workspace_id {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Forbidden" })),
        )
            .into_response();
    }

    match state.settings_repo.get_settings(workspace_id.into()).await {
        Ok(Some(settings)) => {
            let retention_days = settings.traces_retention_days as u32;

            (
                StatusCode::OK,
                Json(serde_json::json!(WorkspaceRetentionResponse {
                    workspace_id: workspace_id.into(),
                    retention_days,
                    inherited: false,
                    workspace_default_days: retention_days,
                })),
            )
                .into_response()
        }
        Ok(None) => {
            let retention_days = state.config_store.get_effective_days(workspace_id.into());
            (
                StatusCode::OK,
                Json(serde_json::json!(WorkspaceRetentionResponse {
                    workspace_id: workspace_id.into(),
                    retention_days,
                    inherited: true,
                    workspace_default_days: retention_days,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn set_workspace_retention(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<SetWorkspaceRetentionRequest>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::WriteSettings) {
        return e.into_response();
    }
    let auth_workspace_id = match auth.workspace_uuid() {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    if auth_workspace_id != workspace_id {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Forbidden" })),
        )
            .into_response();
    }

    let settings = match state.settings_repo.get_settings(workspace_id.into()).await {
        Ok(Some(s)) => zradar_models::NewWorkspaceSettings {
            workspace_id: s.workspace_id,
            traces_retention_days: body.retention_days as i32,
            metrics_retention_days: body.retention_days as i32,
            logs_retention_days: body.retention_days as i32,
            max_ingestion_rate: s.max_ingestion_rate,
            file_push_interval_secs: s.file_push_interval_secs,
            blocked: s.blocked,
            capture_llm_content_enabled: s.capture_llm_content_enabled,
        },
        Ok(None) => {
            let mut s = zradar_models::NewWorkspaceSettings::defaults_for(workspace_id.into());
            s.traces_retention_days = body.retention_days as i32;
            s.metrics_retention_days = body.retention_days as i32;
            s.logs_retention_days = body.retention_days as i32;
            s
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    let saved = match state.settings_repo.upsert_settings(settings).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    state.config_store.upsert(WorkspaceRetentionConfig {
        workspace_id: workspace_id.into(),
        retention_days: body.retention_days,
    });

    (
        StatusCode::OK,
        Json(serde_json::json!(WorkspaceRetentionResponse {
            workspace_id: workspace_id.into(),
            retention_days: body.retention_days,
            inherited: false,
            workspace_default_days: saved.traces_retention_days as u32,
        })),
    )
        .into_response()
}
