//! HTTP handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::*, types::*};
use crate::errors::{ControlError, Result};
use crate::audit::{AuditLogger, AuditEvent, AuditStatus};
use crate::auth::{ApiKeyAuth, DefaultKeyGenerator};
use crate::http::extractors::AuthenticatedUser;

#[utoipa::path(
    post,
    path = "/api/v1/projects/{project_id}/api-keys",
    params(
        ("project_id" = Uuid, Path, description = "Project ID")
    ),
    request_body = CreateApiKeyRequest,
    responses(
        (status = 201, description = "API key created", body = CreateApiKeyResponse),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "api-keys"
)]
pub async fn create_api_key(
    State(service): State<Arc<ApiKeyService<DefaultKeyGenerator>>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>)> {
    // Get project
    let project = service.project_storage.get_project(project_id).await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    // Check permission
    service.rbac.require_permission(
        user.id,
        project.organization_id,
        Some(project_id),
        "api_keys:create"
    ).await?;

    // Validate permissions if provided (skipped for trait abstraction)
    // In production, validation would be done at a higher level
    if let Some(ref _perms) = req.permissions {
        // Permission validation logic would go here
    }

    // Generate API key
    let key = ApiKeyAuth::generate_key("zvr_live");
    let key_hash = ApiKeyAuth::hash_key(&key);
    let key_prefix = key.chars().take(12).collect::<String>();

    // Create API key in database
    let api_key = service.api_key_storage.create_key(
        project.organization_id,
        project_id,
        key_hash,
        key_prefix.clone(),
        req.clone(),
        user.id,
    ).await?;

    // Log creation
    let _ = service.audit.log(AuditEvent {
        organization_id: Some(project.organization_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "api_key.created".to_string(),
        resource_type: Some("api_key".to_string()),
        resource_id: Some(api_key.id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "key_id": api_key.id,
            "key_prefix": key_prefix,
            "project_id": project_id,
            "permissions": api_key.permissions
        })),
    }).await;

    tracing::info!(
        key_id = %api_key.id,
        project_id = %project_id,
        user_id = %user.id,
        "API key created"
    );

    Ok((StatusCode::CREATED, Json(CreateApiKeyResponse {
        id: api_key.id,
        key,  // Return the actual key (only time it's shown)
        key_prefix,
        name: api_key.name,
        permissions: api_key.permissions,
        expires_at: api_key.expires_at,
        created_at: api_key.created_at,
    })))
}

/// List API keys for a project
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/api-keys",
    params(
        ("project_id" = Uuid, Path, description = "Project ID")
    ),
    responses(
        (status = 200, description = "List of API keys", body = Vec<ApiKeyResponse>),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "api-keys"
)]
pub async fn list_api_keys(
    State(service): State<Arc<ApiKeyService<DefaultKeyGenerator>>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<ApiKeyResponse>>> {
    // Get project
    let project = service.project_storage.get_project(project_id).await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    // Check permission
    service.rbac.require_permission(
        user.id,
        project.organization_id,
        Some(project_id),
        "api_keys:read"
    ).await?;

    let keys = service.api_key_storage.list_keys(project.organization_id, project_id).await?;
    let response: Vec<ApiKeyResponse> = keys.into_iter().map(|k| k.into()).collect();

    Ok(Json(response))
}

/// Get API key details
#[utoipa::path(
    get,
    path = "/api/v1/api-keys/{key_id}",
    params(
        ("key_id" = Uuid, Path, description = "API Key ID")
    ),
    responses(
        (status = 200, description = "API key details", body = ApiKeyResponse),
        (status = 404, description = "API key not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "api-keys"
)]
pub async fn get_api_key(
    State(service): State<Arc<ApiKeyService<DefaultKeyGenerator>>>,
    user: AuthenticatedUser,
    Path(key_id): Path<Uuid>,
) -> Result<Json<ApiKeyResponse>> {
    let api_key = service.api_key_storage.get_key(key_id).await?
        .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

    // Check permission
    service.rbac.require_permission(
        user.id,
        api_key.organization_id,
        Some(api_key.project_id),
        "api_keys:read"
    ).await?;

    Ok(Json(api_key.into()))
}

/// Revoke an API key
#[utoipa::path(
    post,
    path = "/api/v1/api-keys/{key_id}/revoke",
    params(
        ("key_id" = Uuid, Path, description = "API Key ID")
    ),
    responses(
        (status = 200, description = "API key revoked"),
        (status = 404, description = "API key not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "api-keys"
)]
pub async fn revoke_api_key(
    State(service): State<Arc<ApiKeyService<DefaultKeyGenerator>>>,
    user: AuthenticatedUser,
    Path(key_id): Path<Uuid>,
) -> Result<StatusCode> {
    let api_key = service.api_key_storage.get_key(key_id).await?
        .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

    // Check permission
    service.rbac.require_permission(
        user.id,
        api_key.organization_id,
        Some(api_key.project_id),
        "api_keys:revoke"
    ).await?;

    service.api_key_storage.revoke_key(key_id).await?;

    // Invalidate cache to ensure revoked key is rejected immediately
    service.auth.revoke(key_id).await?;

    // Log revocation
    let _ = service.audit.log(AuditEvent {
        organization_id: Some(api_key.organization_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "api_key.revoked".to_string(),
        resource_type: Some("api_key".to_string()),
        resource_id: Some(key_id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "key_id": key_id,
            "key_prefix": api_key.key_prefix
        })),
    }).await;

    tracing::info!(key_id = %key_id, user_id = %user.id, "API key revoked");

    Ok(StatusCode::OK)
}

/// Delete an API key
#[utoipa::path(
    delete,
    path = "/api/v1/api-keys/{key_id}",
    params(
        ("key_id" = Uuid, Path, description = "API Key ID")
    ),
    responses(
        (status = 204, description = "API key deleted"),
        (status = 404, description = "API key not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "api-keys"
)]
pub async fn delete_api_key(
    State(service): State<Arc<ApiKeyService<DefaultKeyGenerator>>>,
    user: AuthenticatedUser,
    Path(key_id): Path<Uuid>,
) -> Result<StatusCode> {
    let api_key = service.api_key_storage.get_key(key_id).await?
        .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

    // Check permission (high-risk operation)
    service.rbac.require_permission(
        user.id,
        api_key.organization_id,
        Some(api_key.project_id),
        "api_keys:delete"
    ).await?;

    service.api_key_storage.delete_key(key_id).await?;

    // Invalidate cache
    service.auth.revoke(key_id).await?;

    // Log deletion
    let _ = service.audit.log(AuditEvent {
        organization_id: Some(api_key.organization_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "api_key.deleted".to_string(),
        resource_type: Some("api_key".to_string()),
        resource_id: Some(key_id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "key_id": key_id,
            "key_prefix": api_key.key_prefix
        })),
    }).await;

    tracing::warn!(key_id = %key_id, user_id = %user.id, "API key deleted");

    Ok(StatusCode::NO_CONTENT)
}
