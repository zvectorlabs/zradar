use axum::{Extension, Router, routing};
use std::sync::Arc;
use zradar_traits::Authenticator;

use super::handlers::{AuditState, list_audit_logs};

pub fn audit_router(state: Arc<AuditState>, auth: Arc<dyn Authenticator>) -> Router {
    Router::new()
        .route("/api/v1/admin/audit-logs", routing::get(list_audit_logs))
        .layer(Extension(auth))
        .with_state(state)
}
