//! Telemetry/Query module router

use axum::{Router, routing::get};
use std::sync::Arc;

use super::{QueryService, handlers};

/// Create the query/telemetry router with all endpoints.
pub fn router(service: Arc<QueryService>) -> Router {
    Router::new()
        .route("/api/v1/traces", get(handlers::query_traces))
        .route("/api/v1/traces/:trace_id", get(handlers::get_trace))
        .route("/api/v1/spans", get(handlers::query_spans))
        .route("/api/v1/spans/:span_id", get(handlers::get_span))
        .route("/api/v1/analytics", get(handlers::get_analytics))
        .route(
            "/api/v1/analytics/metrics",
            get(handlers::get_metrics_summary),
        )
        .route(
            "/api/v1/analytics/top-endpoints",
            get(handlers::get_top_endpoints),
        )
        .route(
            "/api/v1/analytics/errors",
            get(handlers::get_error_breakdown),
        )
        .route("/api/v1/analytics/llm", get(handlers::get_llm_analytics))
        .route(
            "/api/v1/analytics/agents",
            get(handlers::get_agent_analytics),
        )
        .route(
            "/api/v1/analytics/storage-usage",
            get(handlers::get_storage_usage),
        )
        .route(
            "/api/v1/analytics/quota-status",
            get(handlers::get_quota_status),
        )
        .route(
            "/api/v1/analytics/usage-daily",
            get(handlers::get_usage_daily),
        )
        .route(
            "/api/v1/analytics/ingest-rate",
            get(handlers::get_ingest_rate),
        )
        .route(
            "/api/v1/analytics/query-usage",
            get(handlers::get_query_usage),
        )
        .route("/api/v1/logs", get(handlers::query_logs))
        .route("/api/v1/logs/:log_id", get(handlers::get_log))
        .route("/api/v1/metrics", get(handlers::query_metrics))
        .route("/api/v1/metrics/series", get(handlers::query_metric_series))
        .with_state(service)
}
