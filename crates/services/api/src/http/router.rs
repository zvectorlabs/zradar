//! Admin API Router
//!
//! Health endpoints (`/health`, `/health/ready`, `/health/live`) are provided by
//! the server's health router; do not add them here to avoid duplicate route panic.

use axum::{Extension, Router};
use std::sync::Arc;
use zradar_traits::Authenticator;

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
/// Authentication: `Authorization: Bearer <api-key>` on every request.
pub fn create_admin_router(
    query_service: Arc<QueryService>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    Router::new()
        .merge(crate::telemetry::router::router(query_service))
        .layer(Extension(auth))
}
