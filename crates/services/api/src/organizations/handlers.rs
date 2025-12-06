//! Organization HTTP handlers

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::OrganizationService, types::*};
use crate::errors::Result;
use crate::http::extractors::AuthenticatedUser;

/// Create a new organization
#[utoipa::path(
    post,
    path = "/api/v1/organizations",
    request_body = CreateOrganizationRequest,
    responses(
        (status = 201, description = "Organization created", body = Organization),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 409, description = "Organization slug already exists"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn create_organization(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Json(req): Json<CreateOrganizationRequest>,
) -> Result<(StatusCode, Json<Organization>)> {
    let org = service.create_organization(user.id, req).await?;
    Ok((StatusCode::CREATED, Json(org)))
}

/// List user's organizations
#[utoipa::path(
    get,
    path = "/api/v1/organizations",
    responses(
        (status = 200, description = "List of organizations", body = Vec<OrganizationWithRole>),
        (status = 401, description = "Unauthorized"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn list_organizations(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
) -> Result<Json<Vec<OrganizationWithRole>>> {
    let orgs = service.list_user_organizations(user.id).await?;
    Ok(Json(orgs))
}

/// Get organization by ID
#[utoipa::path(
    get,
    path = "/api/v1/organizations/{org_id}",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    responses(
        (status = 200, description = "Organization details", body = Organization),
        (status = 404, description = "Organization not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn get_organization(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
) -> Result<Json<Organization>> {
    let org = service.get_organization(user.id, org_id).await?;
    Ok(Json(org))
}

/// Update organization
#[utoipa::path(
    patch,
    path = "/api/v1/organizations/{org_id}",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    request_body = UpdateOrganizationRequest,
    responses(
        (status = 200, description = "Organization updated", body = Organization),
        (status = 404, description = "Organization not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn update_organization(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
    Json(req): Json<UpdateOrganizationRequest>,
) -> Result<Json<Organization>> {
    let org = service.update_organization(user.id, org_id, req).await?;
    Ok(Json(org))
}

/// Delete organization
#[utoipa::path(
    delete,
    path = "/api/v1/organizations/{org_id}",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    responses(
        (status = 204, description = "Organization deleted"),
        (status = 404, description = "Organization not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn delete_organization(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
) -> Result<StatusCode> {
    service.delete_organization(user.id, org_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Add member to organization
#[utoipa::path(
    post,
    path = "/api/v1/organizations/{org_id}/members",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    request_body = AddOrganizationMemberRequest,
    responses(
        (status = 201, description = "Member added", body = OrganizationMember),
        (status = 404, description = "Organization or user not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn add_organization_member(
    State(_service): State<Arc<OrganizationService>>,
    _user: AuthenticatedUser,
    Path(_org_id): Path<Uuid>,
    Json(_req): Json<AddOrganizationMemberRequest>,
) -> Result<(StatusCode, Json<OrganizationMember>)> {
    // TODO: Implement user lookup and add member
    use crate::errors::ControlError;
    Err(ControlError::Internal(
        "User lookup not yet implemented".to_string(),
    ))
}

/// List organization members
#[utoipa::path(
    get,
    path = "/api/v1/organizations/{org_id}/members",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    responses(
        (status = 200, description = "List of members", body = Vec<OrganizationMember>),
        (status = 404, description = "Organization not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn list_organization_members(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
) -> Result<Json<Vec<OrganizationMember>>> {
    let members = service.list_members(user.id, org_id).await?;
    Ok(Json(members))
}

/// Remove member from organization
#[utoipa::path(
    delete,
    path = "/api/v1/organizations/{org_id}/members/{user_id}",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID"),
        ("user_id" = Uuid, Path, description = "User ID to remove")
    ),
    responses(
        (status = 204, description = "Member removed"),
        (status = 404, description = "Organization or member not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Organizations"
)]
pub async fn remove_organization_member(
    State(service): State<Arc<OrganizationService>>,
    user: AuthenticatedUser,
    Path((org_id, target_user_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode> {
    service
        .remove_member(user.id, org_id, target_user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
