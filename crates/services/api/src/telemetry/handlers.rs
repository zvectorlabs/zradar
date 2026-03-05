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

/// Get a single span by its ID
#[utoipa::path(
    get,
    path = "/api/v1/spans/{span_id}",
    params(
        ("span_id" = String, Path, description = "Span ID"),
        ("project_id" = Uuid, Query, description = "Project ID"),
    ),
    responses(
        (status = 200, description = "Span retrieved", body = SpanDetail),
        (status = 404, description = "Span not found"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "query"
)]
pub async fn get_span(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Path(span_id): Path<String>,
    Query(params): Query<ProjectIdParam>,
) -> Result<Json<SpanDetail>> {
    let span = service
        .get_span(user.id, Uuid::nil(), params.project_id, &span_id)
        .await?;
    Ok(Json(span))
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

/// Get metrics summary
#[utoipa::path(
    get,
    path = "/api/v1/analytics/metrics",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start" = String, Query, description = "Start time (ISO 8601)"),
        ("end" = String, Query, description = "End time (ISO 8601)"),
    ),
    responses(
        (status = 200, description = "Metrics summary retrieved", body = MetricsSummary),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(("bearer_token" = [])),
    tag = "Analytics"
)]
pub async fn get_metrics_summary(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(query): Query<AnalyticsQuery>,
) -> Result<Json<MetricsSummary>> {
    let summary = service
        .get_metrics_summary(user.id, Uuid::nil(), query)
        .await?;
    Ok(Json(summary))
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

/// Query log records
#[utoipa::path(
    get,
    path = "/api/v1/logs",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start_time" = Option<String>, Query, description = "Start time (ISO 8601)"),
        ("end_time" = Option<String>, Query, description = "End time (ISO 8601)"),
        ("severity" = Option<String>, Query, description = "Severity filter (INFO, WARN, ERROR, …)"),
        ("service_name" = Option<String>, Query, description = "Service name filter"),
        ("trace_id" = Option<String>, Query, description = "Trace ID filter"),
        ("search_text" = Option<String>, Query, description = "Full-text search in message"),
        ("agent_name" = Option<String>, Query, description = "Agent name filter"),
        ("session_id" = Option<String>, Query, description = "Session ID filter"),
        ("limit" = Option<i64>, Query, description = "Pagination limit"),
    ),
    responses(
        (status = 200, description = "Logs retrieved", body = PaginatedResponse<LogDetail>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
    ),
    security(("bearer_token" = [])),
    tag = "logs"
)]
pub async fn query_logs(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(filters): Query<LogQueryFilters>,
) -> Result<Json<PaginatedResponse<LogDetail>>> {
    let logs = service.query_logs(user.id, Uuid::nil(), filters).await?;
    Ok(Json(logs))
}

/// Get a single log record by ID
#[utoipa::path(
    get,
    path = "/api/v1/logs/{log_id}",
    params(
        ("log_id" = String, Path, description = "Log record ID"),
        ("project_id" = Uuid, Query, description = "Project ID"),
    ),
    responses(
        (status = 200, description = "Log record retrieved", body = LogDetail),
        (status = 404, description = "Log not found"),
        (status = 401, description = "Unauthorized"),
    ),
    security(("bearer_token" = [])),
    tag = "logs"
)]
pub async fn get_log(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Path(log_id): Path<String>,
    Query(params): Query<ProjectIdParam>,
) -> Result<Json<LogDetail>> {
    let log = service
        .get_log(user.id, Uuid::nil(), params.project_id, &log_id)
        .await?;
    Ok(Json(log))
}

/// Query metrics
#[utoipa::path(
    get,
    path = "/api/v1/metrics",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("start_time" = Option<String>, Query, description = "Start time (ISO 8601)"),
        ("end_time" = Option<String>, Query, description = "End time (ISO 8601)"),
        ("metric_name" = Option<String>, Query, description = "Metric name filter"),
        ("service_name" = Option<String>, Query, description = "Service name filter"),
        ("agent_name" = Option<String>, Query, description = "Agent name filter"),
        ("limit" = Option<i64>, Query, description = "Pagination limit"),
    ),
    responses(
        (status = 200, description = "Metrics retrieved", body = PaginatedResponse<MetricDetail>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
    ),
    security(("bearer_token" = [])),
    tag = "metrics"
)]
pub async fn query_metrics(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(filters): Query<MetricQueryFilters>,
) -> Result<Json<PaginatedResponse<MetricDetail>>> {
    let metrics = service
        .query_metrics(user.id, Uuid::nil(), filters)
        .await?;
    Ok(Json(metrics))
}

/// Query metric time-series (bucketed aggregates)
#[utoipa::path(
    get,
    path = "/api/v1/metrics/series",
    params(
        ("project_id" = String, Query, description = "Project ID"),
        ("metric_name" = String, Query, description = "Metric name"),
        ("start_time" = Option<String>, Query, description = "Start time (ISO 8601)"),
        ("end_time" = Option<String>, Query, description = "End time (ISO 8601)"),
        ("interval_seconds" = Option<u64>, Query, description = "Bucket interval in seconds (default: 60)"),
        ("aggregation" = Option<String>, Query, description = "Aggregation: avg, sum, min, max, count (default: avg)"),
        ("service_name" = Option<String>, Query, description = "Service name filter"),
    ),
    responses(
        (status = 200, description = "Metric series retrieved", body = Vec<MetricSeriesPoint>),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
    ),
    security(("bearer_token" = [])),
    tag = "metrics"
)]
pub async fn query_metric_series(
    State(service): State<Arc<QueryService>>,
    user: AuthenticatedUser,
    Query(filters): Query<MetricSeriesFilters>,
) -> Result<Json<Vec<MetricSeriesPoint>>> {
    let series = service
        .query_metric_series(user.id, Uuid::nil(), filters)
        .await?;
    Ok(Json(series))
}
