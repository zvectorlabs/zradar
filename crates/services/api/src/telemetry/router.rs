//! Telemetry/Query module router

use axum::{
    Router,
    routing::get,
    Extension,
};
use std::sync::Arc;

use super::{QueryService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the query/telemetry router with all endpoints
pub fn router(
    service: Arc<QueryService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route("/api/v1/traces", get(handlers::query_traces))
        .route("/api/v1/traces/:trace_id", get(handlers::get_trace))
        .route("/api/v1/spans", get(handlers::query_spans))
        .route("/api/v1/analytics", get(handlers::get_analytics))
        .route("/api/v1/analytics/top-endpoints", get(handlers::get_top_endpoints))
        .route("/api/v1/analytics/errors", get(handlers::get_error_breakdown))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}

