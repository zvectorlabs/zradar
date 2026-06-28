use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;
use zradar_policy::{
    Operation, Policy, PolicyId, PolicyLimit, PolicySource, PolicyStore, SignalKind,
};

use crate::http::{AuthContext, Capability};

pub struct PolicyState {
    pub store: Arc<dyn PolicyStore>,
}

#[derive(Debug, Deserialize)]
pub struct ListPoliciesQuery {
    pub workspace_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertPoliciesRequest {
    #[serde(default)]
    pub workspace_id: Option<Uuid>,
    pub policies: Vec<PolicyConfig>,
}

#[derive(Debug, Deserialize)]
pub struct PolicyConfig {
    #[serde(default)]
    pub workspace_id: Option<Uuid>,
    pub signal: SignalKind,
    pub operation: Operation,
    pub limit: PolicyLimit,
    #[serde(default = "default_grace_pct")]
    pub grace_pct: u8,
    #[serde(default = "default_hard_block_pct")]
    pub hard_block_pct: u8,
    #[serde(default)]
    pub effective_from: Option<i64>,
    #[serde(default)]
    pub effective_until: Option<i64>,
    #[serde(default)]
    pub source: Option<PolicySource>,
}

pub async fn upsert_policies(
    State(state): State<Arc<PolicyState>>,
    auth: AuthContext,
    Json(body): Json<UpsertPoliciesRequest>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }

    let workspace_id = match auth.workspace_or_reject_platform_override(body.workspace_id) {
        Ok(workspace_id) => workspace_id,
        Err(e) => return e.into_response(),
    };
    let now = chrono::Utc::now().timestamp_micros();

    let policies = body
        .policies
        .into_iter()
        .map(|policy_config| Policy {
            id: None,
            workspace_id: workspace_id.into(),

            signal: policy_config.signal,
            operation: policy_config.operation,
            limit: policy_config.limit,
            grace_pct: policy_config.grace_pct,
            hard_block_pct: policy_config.hard_block_pct,
            effective_from: policy_config.effective_from.unwrap_or(now),
            effective_until: policy_config.effective_until,
            source: policy_config.source.unwrap_or(PolicySource::Api),
        })
        .collect();

    if let Err(e) = state.store.upsert_many(policies).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

pub async fn list_policies(
    State(state): State<Arc<PolicyState>>,
    auth: AuthContext,
    Query(query): Query<ListPoliciesQuery>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }

    let workspace_id = match auth.workspace_or_standalone_override(query.workspace_id) {
        Ok(workspace_id) => workspace_id,
        Err(e) => return e.into_response(),
    };

    match state.store.list(workspace_id.into()).await {
        Ok(policies) => (StatusCode::OK, Json(serde_json::json!(policies))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn get_effective_policy(
    State(state): State<Arc<PolicyState>>,
    auth: AuthContext,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }
    if let Err(e) = auth.enforce_path_workspace(workspace_id) {
        return e.into_response();
    }

    let workspace_id = match auth.workspace_uuid() {
        Ok(workspace_id) => workspace_id,
        Err(e) => return e.into_response(),
    };

    let policies = serde_json::json!({
        "ingest": state.store.resolve(workspace_id.into(), SignalKind::All, Operation::Ingest),
        "query": state.store.resolve(workspace_id.into(), SignalKind::All, Operation::Query),
        "store": state.store.resolve(workspace_id.into(), SignalKind::All, Operation::Store),
    });

    (StatusCode::OK, Json(policies)).into_response()
}

pub async fn delete_policy(
    State(state): State<Arc<PolicyState>>,
    auth: AuthContext,
    Path(id): Path<i64>,
    Query(query): Query<ListPoliciesQuery>,
) -> impl IntoResponse {
    if let Err(e) = auth.require(Capability::Admin) {
        return e.into_response();
    }

    let workspace_id = match auth.workspace_or_standalone_override(query.workspace_id) {
        Ok(workspace_id) => workspace_id,
        Err(e) => return e.into_response(),
    };

    match state.store.list(workspace_id.into()).await {
        Ok(policies) => {
            if !policies
                .iter()
                .any(|policy| policy.id == Some(PolicyId(id)))
            {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    }

    match state.store.delete(PolicyId(id)).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

fn default_grace_pct() -> u8 {
    101
}

fn default_hard_block_pct() -> u8 {
    103
}
