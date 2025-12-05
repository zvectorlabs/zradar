//! HTTP handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::RoleService, types::*};
use crate::errors::{ControlError, Result};
use crate::audit::{AuditLogger, AuditEvent, AuditStatus};
use crate::http::extractors::AuthenticatedUser;
use tracing::info;

#[utoipa::path(
    post,
    path = "/api/v1/organizations/{org_id}/roles",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    request_body = CreateCustomRoleRequest,
    responses(
        (status = 201, description = "Custom role created successfully", body = CustomRole),
        (status = 400, description = "Invalid request or permission validation failed"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(("bearer_token" = []))
)]
pub async fn create_role(
    State(service): State<Arc<RoleService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
    Json(req): Json<CreateCustomRoleRequest>,
) -> Result<(StatusCode, Json<CustomRole>)> {
    info!("Creating custom role: {} in org {}", req.name, org_id);

    // Check permission
    service.rbac
        .check_permission(user.id, org_id, None, "admin:roles")
        .await?;

    // Basic permission validation (ensure not empty)
    if req.permissions.is_empty() {
        return Err(ControlError::InvalidInput("Role must have at least one permission".to_string()));
    }

    // Create role
    let role = service.role_storage.create_custom_role(org_id, req.clone(), user.id).await?;

    // Audit log
    service.audit.log(AuditEvent {
        organization_id: Some(org_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "create".to_string(),
        resource_type: Some("custom_role".to_string()),
        resource_id: Some(role.id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "role_name": role.name,
            "permissions": role.permissions,
        })),
    }).await?;

    info!("Created custom role: {} ({})", role.name, role.id);

    Ok((StatusCode::CREATED, Json(role)))
}

/// List custom roles for an organization
#[utoipa::path(
    get,
    path = "/api/v1/organizations/{org_id}/roles",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    responses(
        (status = 200, description = "List of custom roles", body = Vec<CustomRole>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Insufficient permissions"),
        (status = 404, description = "Organization not found"),
    ),
    security(("bearer_token" = []))
)]
pub async fn list_roles(
    State(service): State<Arc<RoleService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
) -> Result<Json<Vec<CustomRole>>> {
    // Check permission
    service.rbac
        .check_permission(user.id, org_id, None, "read:roles")
        .await?;

    let roles = service.role_storage.list_custom_roles(org_id, None).await?;

    Ok(Json(roles))
}

/// Get a specific custom role
#[utoipa::path(
    get,
    path = "/api/v1/roles/{role_id}",
    params(
        ("role_id" = Uuid, Path, description = "Role ID")
    ),
    responses(
        (status = 200, description = "Custom role details", body = CustomRole),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Insufficient permissions"),
        (status = 404, description = "Role not found"),
    ),
    security(("bearer_token" = []))
)]
pub async fn get_custom_role(
    State(service): State<Arc<RoleService>>,
    user: AuthenticatedUser,
    Path(role_id): Path<Uuid>,
) -> Result<Json<CustomRole>> {
    let role = service.role_storage.get_custom_role(role_id).await?
        .ok_or(ControlError::NotFound("Role not found".to_string()))?;

    // Check permission
    service.rbac
        .check_permission(user.id, role.organization_id, None, "read:roles")
        .await?;

    Ok(Json(role))
}

/// Update a custom role
#[utoipa::path(
    put,
    path = "/api/v1/roles/{role_id}",
    params(
        ("role_id" = Uuid, Path, description = "Role ID")
    ),
    request_body = UpdateCustomRoleRequest,
    responses(
        (status = 200, description = "Custom role updated successfully", body = CustomRole),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Insufficient permissions"),
        (status = 404, description = "Role not found"),
    ),
    security(("bearer_token" = []))
)]
pub async fn update_custom_role(
    State(service): State<Arc<RoleService>>,
    user: AuthenticatedUser,
    Path(role_id): Path<Uuid>,
    Json(req): Json<UpdateCustomRoleRequest>,
) -> Result<Json<CustomRole>> {
    let existing_role = service.role_storage.get_custom_role(role_id).await?
        .ok_or(ControlError::NotFound("Role not found".to_string()))?;

    // Check permission
    service.rbac
        .check_permission(user.id, existing_role.organization_id, None, "admin:roles")
        .await?;

    // Validate new permissions if provided
    if let Some(ref perms) = req.permissions
        && perms.is_empty() {
            return Err(ControlError::InvalidInput("Role must have at least one permission".to_string()));
        }

    // Update role
    let updated_role = service.role_storage.update_custom_role(role_id, req.clone()).await?;

    // Audit log
    service.audit.log(AuditEvent {
        organization_id: Some(existing_role.organization_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "update".to_string(),
        resource_type: Some("custom_role".to_string()),
        resource_id: Some(role_id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "changes": req,
        })),
    }).await?;

    info!("Updated custom role: {}", role_id);

    Ok(Json(updated_role))
}

/// Delete a custom role
#[utoipa::path(
    delete,
    path = "/api/v1/roles/{role_id}",
    params(
        ("role_id" = Uuid, Path, description = "Role ID")
    ),
    responses(
        (status = 204, description = "Custom role deleted successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Insufficient permissions"),
        (status = 404, description = "Role not found"),
    ),
    security(("bearer_token" = []))
)]
pub async fn delete_custom_role(
    State(service): State<Arc<RoleService>>,
    user: AuthenticatedUser,
    Path(role_id): Path<Uuid>,
) -> Result<StatusCode> {
    let role = service.role_storage.get_custom_role(role_id).await?
        .ok_or(ControlError::NotFound("Role not found".to_string()))?;

    // Check permission
    service.rbac
        .check_permission(user.id, role.organization_id, None, "admin:roles")
        .await?;

    // Delete role
    service.role_storage.delete_custom_role(role_id).await?;

    // Audit log
    service.audit.log(AuditEvent {
        organization_id: Some(role.organization_id),
        user_id: Some(user.id),
        actor_type: Some("user".to_string()),
        actor_id: Some(user.id),
        actor_ip: None,
        action: "delete".to_string(),
        resource_type: Some("custom_role".to_string()),
        resource_id: Some(role_id),
        status: AuditStatus::Success,
        details: Some(serde_json::json!({
            "role_name": role.name,
        })),
    }).await?;

    info!("Deleted custom role: {}", role_id);

    Ok(StatusCode::NO_CONTENT)
}

/// List all available permission definitions
#[utoipa::path(
    get,
    path = "/api/v1/permissions",
    responses(
        (status = 200, description = "List of available permissions", body = Vec<PermissionDefinition>),
        (status = 401, description = "Unauthorized"),
    ),
    security(("bearer_token" = []))
)]
pub async fn list_permissions(
    State(service): State<Arc<RoleService>>,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<PermissionDefinition>>> {
    let permissions = service.role_storage.get_permission_definitions(None).await?;

    Ok(Json(permissions))
}
