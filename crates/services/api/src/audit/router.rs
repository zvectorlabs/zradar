use axum::{Extension, Router, routing};
use std::sync::Arc;
use zradar_traits::Authenticator;

use super::handlers::{AuditState, list_audit_logs};
use crate::http::AuthMode;

pub fn audit_router(
    state: Arc<AuditState>,
    auth: Arc<dyn Authenticator>,
    auth_mode: AuthMode,
) -> Router {
    Router::new()
        .route("/api/v1/admin/audit-logs", routing::get(list_audit_logs))
        .layer(Extension(auth_mode))
        .layer(Extension(auth))
        .with_state(state)
}
