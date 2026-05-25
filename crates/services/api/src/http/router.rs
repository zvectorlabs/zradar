//! Admin API Router
//!
//! Health endpoints (`/health`, `/health/ready`, `/health/live`) are provided by
//! the server's health router; do not add them here to avoid duplicate route panic.

use axum::{Extension, Router};
use std::sync::Arc;
use zradar_traits::Authenticator;

use crate::http::auth_extractor::AuthMode;
use crate::telemetry::QueryService;

/// Create the admin API router.
///
/// Routes:
/// - `/api/v1/traces*`       — telemetry query
/// - `/api/v1/spans*`        — span query
/// - `/api/v1/analytics*`    — analytics
/// - `/api/v1/logs*`         — log query
/// - `/api/v1/metrics*`      — metrics query
///
/// Authentication:
/// - Standalone: `Authorization: Bearer <api-key>`
/// - Platform:   `Authorization: Bearer <gateway-service-token>` + trusted context headers
pub fn create_admin_router(
    query_service: Arc<QueryService>,
    auth: Arc<dyn Authenticator>,
    auth_mode: AuthMode,
) -> Router {
    Router::new()
        .merge(crate::telemetry::router::router(query_service))
        .layer(Extension(auth_mode))
        .layer(Extension(auth))
}
