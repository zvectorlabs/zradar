//! HTTP handlers

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::ScoresService, types::*};
use crate::errors::{ControlError, Result};
use crate::http::extractors::AuthenticatedUser;

#[utoipa::path(
    post,
    path = "/api/v1/projects/{project_id}/scores",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
    ),
    request_body = CreateScoreRequest,
    responses(
        (status = 201, description = "Score created", body = ScoreResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn create_score(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path(project_id): Path<Uuid>,
    Json(request): Json<CreateScoreRequest>,
) -> Result<(StatusCode, Json<ScoreResponse>)> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    // Debug logging in test mode
    if std::env::var("ZVRADAR_TEST_MODE").is_ok() {
        tracing::info!(
            "🔍 CREATE SCORE: project_id={}, org_id={}, trace_id={}, name={}",
            project_id,
            project.organization_id,
            request.trace_id,
            request.name
        );
    }

    let response = service
        .create_score(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            request,
        )
        .await?;

    if std::env::var("ZVRADAR_TEST_MODE").is_ok() {
        tracing::info!("🔍 SCORE CREATED: id={}", response.id);
    }

    Ok((StatusCode::CREATED, Json(response)))
}

/// Get evaluation scores for a trace
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/traces/{trace_id}/scores",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
        ("trace_id" = String, Path, description = "Trace ID"),
    ),
    responses(
        (status = 200, description = "Scores retrieved", body = Vec<ScoreResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn get_trace_scores(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path((project_id, trace_id)): Path<(Uuid, String)>,
) -> Result<Json<Vec<ScoreResponse>>> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    // Debug logging in test mode
    if std::env::var("ZVRADAR_TEST_MODE").is_ok() {
        tracing::info!(
            "🔍 GET TRACE SCORES: project_id={}, org_id={}, trace_id={}",
            project_id,
            project.organization_id,
            trace_id
        );
    }

    let scores = service
        .get_trace_scores(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            &trace_id,
        )
        .await?;

    if std::env::var("ZVRADAR_TEST_MODE").is_ok() {
        tracing::info!("🔍 FOUND {} scores for trace_id={}", scores.len(), trace_id);
    }

    Ok(Json(scores))
}

/// Get score summary for a trace
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/traces/{trace_id}/scores/summary",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
        ("trace_id" = String, Path, description = "Trace ID"),
    ),
    responses(
        (status = 200, description = "Score summary retrieved", body = Vec<ScoreSummaryResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn get_trace_score_summary(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path((project_id, trace_id)): Path<(Uuid, String)>,
) -> Result<Json<Vec<ScoreSummaryResponse>>> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    let summary = service
        .get_trace_score_summary(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            &trace_id,
        )
        .await?;

    Ok(Json(summary))
}

/// Get evaluation scores for a session
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/sessions/{session_id}/scores",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
        ("session_id" = String, Path, description = "Session ID"),
    ),
    responses(
        (status = 200, description = "Scores retrieved", body = Vec<ScoreResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn get_session_scores(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path((project_id, session_id)): Path<(Uuid, String)>,
) -> Result<Json<Vec<ScoreResponse>>> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    let scores = service
        .get_session_scores(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            &session_id,
        )
        .await?;

    Ok(Json(scores))
}

/// Get single score by ID
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/scores/{score_id}",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
        ("score_id" = String, Path, description = "Score ID"),
    ),
    responses(
        (status = 200, description = "Score retrieved", body = ScoreResponse),
        (status = 404, description = "Score not found"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn get_score_by_id(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path((project_id, score_id)): Path<(Uuid, String)>,
) -> Result<Json<ScoreResponse>> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    let score = service
        .get_score_by_id(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            &score_id,
        )
        .await?;

    Ok(Json(score))
}

/// Delete an evaluation score
#[utoipa::path(
    delete,
    path = "/api/v1/projects/{project_id}/scores/{score_id}",
    params(
        ("project_id" = Uuid, Path, description = "Project ID"),
        ("score_id" = String, Path, description = "Score ID"),
    ),
    responses(
        (status = 204, description = "Score deleted"),
        (status = 404, description = "Score not found"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "Scores"
)]
pub async fn delete_score(
    State(service): State<Arc<ScoresService>>,
    user: AuthenticatedUser,
    Path((project_id, score_id)): Path<(Uuid, String)>,
) -> Result<StatusCode> {
    // Look up project to get organization_id (tenant_id)
    let project = service
        .project_repository
        .get_project(project_id)
        .await?
        .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

    service
        .delete_score(
            user.id,
            project.organization_id.to_string(), // Use organization_id as tenant_id
            project_id,
            &score_id,
        )
        .await?;

    Ok(StatusCode::NO_CONTENT)
}
