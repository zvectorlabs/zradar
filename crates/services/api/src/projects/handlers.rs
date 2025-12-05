//! Project HTTP handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::ProjectService, types::*};
use crate::errors::Result;
use crate::http::extractors::AuthenticatedUser;

/// Create a new project
#[utoipa::path(
    post,
    path = "/api/v1/organizations/{org_id}/projects",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created", body = Project),
        (status = 400, description = "Invalid request"),
        (status = 403, description = "Insufficient permissions"),
        (status = 409, description = "Project slug already exists"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn create_project(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>)> {
    let project = service.create_project(user.id, org_id, req).await?;
    Ok((StatusCode::CREATED, Json(project)))
}

/// List projects in an organization
#[utoipa::path(
    get,
    path = "/api/v1/organizations/{org_id}/projects",
    params(
        ("org_id" = Uuid, Path, description = "Organization ID")
    ),
    responses(
        (status = 200, description = "List of projects", body = Vec<Project>),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn list_projects(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(org_id): Path<Uuid>,
) -> Result<Json<Vec<Project>>> {
    let projects = service.list_projects(user.id, org_id).await?;
    Ok(Json(projects))
}

/// Get project by ID
#[utoipa::path(
    get,
    path = "/api/v1/projects/{id}",
    params(
        ("id" = Uuid, Path, description = "Project ID")
    ),
    responses(
        (status = 200, description = "Project details", body = Project),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn get_project(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Project>> {
    let project = service.get_project(user.id, project_id).await?;
    Ok(Json(project))
}

/// Update project
#[utoipa::path(
    patch,
    path = "/api/v1/projects/{id}",
    params(
        ("id" = Uuid, Path, description = "Project ID")
    ),
    request_body = UpdateProjectRequest,
    responses(
        (status = 200, description = "Project updated", body = Project),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn update_project(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
    Json(req): Json<UpdateProjectRequest>,
) -> Result<Json<Project>> {
    let project = service.update_project(user.id, project_id, req).await?;
    Ok(Json(project))
}

/// Delete project
#[utoipa::path(
    delete,
    path = "/api/v1/projects/{id}",
    params(
        ("id" = Uuid, Path, description = "Project ID")
    ),
    responses(
        (status = 204, description = "Project deleted"),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn delete_project(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
) -> Result<StatusCode> {
    service.delete_project(user.id, project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Add member to project
#[utoipa::path(
    post,
    path = "/api/v1/projects/{id}/members",
    params(
        ("id" = Uuid, Path, description = "Project ID")
    ),
    request_body = AddProjectMemberRequest,
    responses(
        (status = 201, description = "Member added", body = ProjectMember),
        (status = 404, description = "Project or user not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn add_project_member(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
    Json(req): Json<AddProjectMemberRequest>,
) -> Result<(StatusCode, Json<ProjectMember>)> {
    let member = service.add_member(user.id, project_id, req).await?;
    Ok((StatusCode::CREATED, Json(member)))
}

/// List project members
#[utoipa::path(
    get,
    path = "/api/v1/projects/{id}/members",
    params(
        ("id" = Uuid, Path, description = "Project ID")
    ),
    responses(
        (status = 200, description = "List of members", body = Vec<ProjectMember>),
        (status = 404, description = "Project not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn list_project_members(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
) -> Result<Json<Vec<ProjectMember>>> {
    let members = service.list_members(user.id, project_id).await?;
    Ok(Json(members))
}

/// Remove member from project
#[utoipa::path(
    delete,
    path = "/api/v1/projects/{id}/members/{user_id}",
    params(
        ("id" = Uuid, Path, description = "Project ID"),
        ("user_id" = Uuid, Path, description = "User ID to remove")
    ),
    responses(
        (status = 204, description = "Member removed"),
        (status = 404, description = "Project or member not found"),
        (status = 403, description = "Insufficient permissions"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "projects"
)]
pub async fn remove_project_member(
    State(service): State<Arc<ProjectService>>,
    user: AuthenticatedUser,
    Path((project_id, target_user_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode> {
    service.remove_member(user.id, project_id, target_user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
