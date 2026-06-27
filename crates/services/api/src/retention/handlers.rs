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

use zradar_retention::{
    CleanupJob, FileReclaimer, OrgRetentionConfig, RetentionConfigStore, RetentionRunStats,
};
use zradar_traits::{AuditLogRepository, RetentionPolicyRepository};

use crate::http::{AuthContext, Capability};

/// Shared state for retention handlers.
pub struct RetentionState {
    pub cleanup_job: Arc<CleanupJob>,
    pub file_reclaimer: Arc<FileReclaimer>,
    pub config_store: Arc<RetentionConfigStore>,
    pub policy_repo: Arc<dyn RetentionPolicyRepository>,
    pub audit_log_repo: Option<Arc<dyn AuditLogRepository>>,
}

/// Query parameters for `POST /api/v1/admin/retention/run`.
#[derive(Debug, Deserialize)]
pub struct RunCleanupParams {
    /// Override retention in days for this run only.
    /// When set, all files older than this many days are deleted regardless
    /// of the stored per-org config.  0 means delete everything.
    pub retention_days: Option<u32>,
    /// If provided, restrict cleanup to this org.
    pub org_id: Option<Uuid>,
    /// If provided, restrict cleanup to this project within the resolved org.
    pub project_id: Option<Uuid>,
}

/// Response body for a cleanup run.
#[derive(Debug, Serialize)]
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
        ("org_id" = Option<Uuid>, Query, description = "Restrict to a specific org"),
        ("project_id" = Option<Uuid>, Query, description = "Restrict to a specific project"),
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

    // Platform mode: ignore org_id override from params — always scope to ctx tenant.
    // Standalone mode: allow org_id override for intra-org admin operations.
    let org_id = match auth.tenant_or_standalone_override(params.org_id) {
        Ok(org_id) => org_id,
        Err(e) => return e.into_response(),
    };

    if let Some(days) = params.retention_days {
        if let Some(project_id) = params.project_id {
            state
                .config_store
                .upsert_project_override(org_id, project_id, days);
        } else {
            state.config_store.upsert(OrgRetentionConfig {
                org_id,
                default_days: days,
                project_overrides: Default::default(),
            });
        }
    }

    // Policy pass: mark expired files deleted=true, then reclaim pass: physically
    // remove soft-deleted files lease-aware. Admin callers see combined stats;
    // `files_deleted` counts physical reclaims (backward-compatible field name).
    let mark_stats = match state
        .cleanup_job
        .run_now_scoped(Some(org_id), params.project_id)
        .await
    {
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
        .run_now_scoped(Some(org_id), params.project_id)
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

/// Request body for `PUT /api/v1/admin/retention/config`.
#[derive(Debug, Deserialize)]
pub struct SetRetentionConfigRequest {
    #[serde(default)]
    pub org_id: Option<Uuid>,
    pub default_days: u32,
    #[serde(default)]
    pub project_overrides: std::collections::HashMap<Uuid, u32>,
}

#[derive(Debug, Serialize)]
pub struct RetentionConfigResponse {
    pub org_id: Uuid,
    pub default_days: u32,
    pub project_overrides: std::collections::HashMap<Uuid, u32>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SetProjectRetentionRequest {
    pub retention_days: u32,
}

#[derive(Debug, Serialize)]
pub struct ProjectRetentionResponse {
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub retention_days: u32,
    pub inherited: bool,
    pub org_default_days: u32,
}

/// `PUT /api/v1/admin/retention/config`
///
/// Set or update the retention configuration for an organisation.
#[utoipa::path(
    put,
    path = "/api/v1/admin/retention/config",
    responses(
        (status = 204, description = "Config updated"),
    ),
    tag = "retention"
)]
pub async fn set_retention_config(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Json(body): Json<SetRetentionConfigRequest>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }

    let org_id = match auth.tenant_or_reject_platform_override(body.org_id) {
        Ok(org_id) => org_id,
        Err(e) => return e.into_response(),
    };

    let saved = match state
        .policy_repo
        .upsert_policy(zradar_models::NewRetentionPolicy {
            org_id,
            default_days: body.default_days as i32,
            project_overrides: body.project_overrides.clone(),
        })
        .await
    {
        Ok(policy) => policy,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    state.config_store.upsert(OrgRetentionConfig {
        org_id,
        default_days: body.default_days,
        project_overrides: body.project_overrides.clone(),
    });

    if let Some(audit_log_repo) = &state.audit_log_repo {
        let actor_tenant_id = auth.tenant_uuid().ok();
        let actor_project_id = auth.project_uuid().ok();
        if let Err(e) = audit_log_repo
            .create_audit_log(zradar_models::NewAuditLog {
                actor_tenant_id,
                actor_project_id,
                org_id: Some(org_id),
                project_id: None,
                action: "retention_config.update".to_string(),
                resource_type: "retention_policy".to_string(),
                resource_id: org_id.to_string(),
                metadata: serde_json::json!({
                    "default_days": saved.default_days,
                    "project_overrides": saved.project_overrides_map().unwrap_or_default(),
                }),
            })
            .await
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!(RetentionConfigResponse {
            org_id: saved.org_id,
            default_days: saved.default_days as u32,
            project_overrides: saved.project_overrides_map().unwrap_or_default(),
            updated_at: Some(saved.updated_at),
        })),
    )
        .into_response()
}

pub async fn get_retention_config(
    State(state): State<Arc<RetentionState>>,
    Path(org_id): Path<Uuid>,
) -> impl IntoResponse {
    match state.policy_repo.get_policy(org_id).await {
        Ok(Some(policy)) => {
            let project_overrides = match policy.project_overrides_map() {
                Ok(overrides) => overrides,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            };

            (
                StatusCode::OK,
                Json(serde_json::json!(RetentionConfigResponse {
                    org_id: policy.org_id,
                    default_days: policy.default_days as u32,
                    project_overrides,
                    updated_at: Some(policy.updated_at),
                })),
            )
                .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Retention config not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn list_retention_configs(State(state): State<Arc<RetentionState>>) -> impl IntoResponse {
    match state.policy_repo.list_policies().await {
        Ok(policies) => {
            let mut responses = Vec::with_capacity(policies.len());
            for policy in policies {
                let project_overrides = match policy.project_overrides_map() {
                    Ok(overrides) => overrides,
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({ "error": e.to_string() })),
                        )
                            .into_response();
                    }
                };

                responses.push(RetentionConfigResponse {
                    org_id: policy.org_id,
                    default_days: policy.default_days as u32,
                    project_overrides,
                    updated_at: Some(policy.updated_at),
                });
            }

            (StatusCode::OK, Json(serde_json::json!(responses))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn get_project_retention(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Path(project_id): Path<Uuid>,
) -> impl IntoResponse {
    let org_id = match auth.tenant_uuid() {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    match state.policy_repo.get_policy(org_id).await {
        Ok(Some(policy)) => {
            let project_overrides = match policy.project_overrides_map() {
                Ok(overrides) => overrides,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            };
            let retention_days = project_overrides
                .get(&project_id)
                .copied()
                .unwrap_or(policy.default_days as u32);

            (
                StatusCode::OK,
                Json(serde_json::json!(ProjectRetentionResponse {
                    org_id: policy.org_id,
                    project_id,
                    retention_days,
                    inherited: !project_overrides.contains_key(&project_id),
                    org_default_days: policy.default_days as u32,
                })),
            )
                .into_response()
        }
        Ok(None) => {
            let retention_days = state.config_store.get_effective_days(org_id, project_id);
            (
                StatusCode::OK,
                Json(serde_json::json!(ProjectRetentionResponse {
                    org_id,
                    project_id,
                    retention_days,
                    inherited: true,
                    org_default_days: retention_days,
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

pub async fn set_project_retention(
    State(state): State<Arc<RetentionState>>,
    auth: AuthContext,
    Path(project_id): Path<Uuid>,
    Json(body): Json<SetProjectRetentionRequest>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::WriteSettings) {
        return e.into_response();
    }
    let org_id = match auth.tenant_uuid() {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let (default_days, mut project_overrides) = match state.policy_repo.get_policy(org_id).await {
        Ok(Some(policy)) => {
            let overrides = match policy.project_overrides_map() {
                Ok(overrides) => overrides,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            };
            (policy.default_days as u32, overrides)
        }
        Ok(None) => (
            state.config_store.get_effective_days(org_id, project_id),
            Default::default(),
        ),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    project_overrides.insert(project_id, body.retention_days);

    let saved = match state
        .policy_repo
        .upsert_policy(zradar_models::NewRetentionPolicy {
            org_id,
            default_days: default_days as i32,
            project_overrides: project_overrides.clone(),
        })
        .await
    {
        Ok(policy) => policy,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    state.config_store.upsert(OrgRetentionConfig {
        org_id,
        default_days,
        project_overrides,
    });

    (
        StatusCode::OK,
        Json(serde_json::json!(ProjectRetentionResponse {
            org_id: saved.org_id,
            project_id,
            retention_days: body.retention_days,
            inherited: false,
            org_default_days: saved.default_days as u32,
        })),
    )
        .into_response()
}
