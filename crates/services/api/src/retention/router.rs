//! Retention admin router.

use axum::{Extension, Router, routing};
use std::sync::Arc;
use zradar_traits::AdminAuthorizer;

use super::handlers::{
    RetentionState, get_project_retention, get_retention_config, list_retention_configs,
    run_cleanup, set_project_retention, set_retention_config,
};
use crate::http::AuthMode;

/// Build the retention admin router.
///
/// Routes:
/// - `POST /api/v1/admin/retention/run`   — trigger cleanup
/// - `PUT  /api/v1/admin/retention/config` — update org retention config
///
/// Authentication: `Authorization: Bearer <api-key>` validated by `AdminAuthorizer`.
pub fn retention_router(
    state: Arc<RetentionState>,
    auth: Arc<dyn AdminAuthorizer>,
    auth_mode: AuthMode,
) -> Router {
    Router::new()
        .route("/api/v1/admin/retention/run", routing::post(run_cleanup))
        .route(
            "/api/v1/admin/retention/config",
            routing::get(list_retention_configs).put(set_retention_config),
        )
        .route(
            "/api/v1/admin/retention/config/:org_id",
            routing::get(get_retention_config),
        )
        .route(
            "/api/v1/projects/:id/retention",
            routing::get(get_project_retention).put(set_project_retention),
        )
        .layer(Extension(auth_mode))
        .layer(Extension(auth))
        .with_state(state)
}
