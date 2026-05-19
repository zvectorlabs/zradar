use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::AuditLog;
use zradar_traits::{AuditLogFilters, AuditLogRepository};

use crate::errors::Result;
use crate::http::auth_extractor::AuthContext;

pub struct AuditState {
    pub repository: Arc<dyn AuditLogRepository>,
}

#[derive(Debug, Deserialize)]
pub struct AuditLogQuery {
    pub org_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub action: Option<String>,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub start_created_at: Option<i64>,
    pub end_created_at: Option<i64>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AuditLogResponse {
    pub items: Vec<AuditLog>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

pub async fn list_audit_logs(
    State(state): State<Arc<AuditState>>,
    AuthContext(_ctx): AuthContext,
    Query(query): Query<AuditLogQuery>,
) -> Result<Json<AuditLogResponse>> {
    let page = state
        .repository
        .list_audit_logs(AuditLogFilters {
            org_id: query.org_id,
            project_id: query.project_id,
            action: query.action,
            resource_type: query.resource_type,
            resource_id: query.resource_id,
            start_created_at: query.start_created_at,
            end_created_at: query.end_created_at,
            limit: query.limit,
            offset: query.offset,
        })
        .await?;

    Ok(Json(AuditLogResponse {
        items: page.items,
        total: page.total,
        limit: page.limit,
        offset: page.offset,
    }))
}
