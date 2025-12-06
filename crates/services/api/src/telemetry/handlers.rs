//! Telemetry/Query HTTP handlers

use axum::{
    Json,
    extract::{Path, Query, State},
};
use std::sync::Arc;
use uuid::Uuid;

use super::{service::QueryService, types::*};
use crate::errors::Result;
use crate::http::extractors::AuthenticatedUser;

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ProjectIdParam {
    pub project_id: Uuid,
}

/// Query traces
#[utoipa::path(
    get,
    path = "/api/v1/traces",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
        ("name" = Option<String>, Query, description = "Trace name filter"),
        ("status" = Option<String>, Query, description = "Status filter"),
        ("service_name" = Option<String>, Query, description = "Service name filter"),
        ("min_duration_ms" = Option<i64>, Query, description = "Minimum duration"),
        ("max_duration_ms" = Option<i64>, Query, description = "Maximum duration"),
        ("llm_model" = Option<String>, Query, description = "LLM model filter"),
        ("llm_provider" = Option<String>, Query, description = "LLM provider filter"),
        ("has_error" = Option<bool>, Query, description = "Filter traces with errors"),
        ("offset" = Option<u64>, Query, description = "Pagination offset"),
        ("limit" = Option<u64>, Query, description = "Pagination limit"),
        ("sort" = Option<String>, Query, description = "Sort order"),
    ),
    responses(
        (status = 200, description = "Traces retrieved", body = PaginatedResponse<TraceSummary>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Query"
)]
pub async fn query_traces(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(filters): Query<TraceQueryFilters>,
) -> Result<Json<PaginatedResponse<TraceSummary>>> {
    // Use Uuid::nil() as org_id since we're checking project permissions
    let traces = service.query_traces(user.id, Uuid::nil(), filters).await?;
    Ok(Json(traces))
}

/// Get trace detail
#[utoipa::path(
    get,
    path = "/api/v1/traces/{trace_id}",
    params(
        ("trace_id" = String, Path, description = "Trace ID"),
        ("project_id" = Uuid, Query, description = "Project ID"),
    ),
    responses(
        (status = 200, description = "Trace detail retrieved", body = TraceDetail),
        (status = 404, description = "Trace not found"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Query"
)]
pub async fn get_trace(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Path(trace_id): Path<String>,
    Query(params): Query<ProjectIdParam>,
) -> Result<Json<TraceDetail>> {
    let trace = service
        .get_trace(user.id, Uuid::nil(), params.project_id, &trace_id)
        .await?;
    Ok(Json(trace))
}

/// Query spans
#[utoipa::path(
    get,
    path = "/api/v1/spans",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
        ("trace_id" = Option<String>, Query, description = "Filter by trace ID"),
        ("name" = Option<String>, Query, description = "Span name filter"),
        ("service_name" = Option<String>, Query, description = "Service name filter"),
        ("span_type" = Option<String>, Query, description = "Filter by single span type (SPAN, EVENT, GENERATION, etc.)"),
        ("span_types" = Option<String>, Query, description = "Filter by multiple span types (comma-separated)"),
        ("min_duration_ms" = Option<i64>, Query, description = "Minimum duration"),
        ("max_duration_ms" = Option<i64>, Query, description = "Maximum duration"),
        ("offset" = Option<u64>, Query, description = "Pagination offset"),
        ("limit" = Option<u64>, Query, description = "Pagination limit"),
    ),
    responses(
        (status = 200, description = "Spans retrieved", body = PaginatedResponse<SpanDetail>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Query"
)]
pub async fn query_spans(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(filters): Query<SpanQueryFilters>,
) -> Result<Json<PaginatedResponse<SpanDetail>>> {
    let spans = service.query_spans(user.id, Uuid::nil(), filters).await?;
    Ok(Json(spans))
}

/// Get analytics
#[utoipa::path(
    get,
    path = "/api/v1/analytics",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
        ("bucket" = Option<String>, Query, description = "Time bucket"),
        ("group_by" = Option<String>, Query, description = "Group by field"),
    ),
    responses(
        (status = 200, description = "Analytics retrieved", body = Vec<AnalyticsResult>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Analytics"
)]
pub async fn get_analytics(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(query): Query<AnalyticsQuery>,
) -> Result<Json<Vec<AnalyticsResult>>> {
    let results = service.get_analytics(user.id, Uuid::nil(), query).await?;
    Ok(Json(results))
}

/// Get top endpoints
#[utoipa::path(
    get,
    path = "/api/v1/analytics/top-endpoints",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
        ("limit" = Option<u32>, Query, description = "Number of results"),
    ),
    responses(
        (status = 200, description = "Top endpoints retrieved", body = Vec<TopEndpoint>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Analytics"
)]
pub async fn get_top_endpoints(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(query): Query<TopNQuery>,
) -> Result<Json<Vec<TopEndpoint>>> {
    let results = service
        .get_top_endpoints(user.id, Uuid::nil(), query)
        .await?;
    Ok(Json(results))
}

/// Get error breakdown
#[utoipa::path(
    get,
    path = "/api/v1/analytics/errors",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
    ),
    responses(
        (status = 200, description = "Error breakdown retrieved", body = Vec<ErrorBreakdown>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Analytics"
)]
pub async fn get_error_breakdown(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(query): Query<ErrorAnalyticsQuery>,
) -> Result<Json<Vec<ErrorBreakdown>>> {
    let results = service
        .get_error_breakdown(user.id, Uuid::nil(), query)
        .await?;
    Ok(Json(results))
}
