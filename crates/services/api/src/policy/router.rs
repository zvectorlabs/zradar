use axum::{Extension, Router, routing};
use std::sync::Arc;
use zradar_traits::AdminAuthorizer;

use super::handlers::{
    PolicyState, delete_policy, get_effective_policy, list_policies, upsert_policies,
};
use crate::http::AuthMode;

pub fn policy_router(
    state: Arc<PolicyState>,
    auth: Arc<dyn AdminAuthorizer>,
    auth_mode: AuthMode,
) -> Router {
    Router::new()
        .route(
            "/api/v1/admin/policies/config",
            routing::get(list_policies).put(upsert_policies),
        )
        .route(
            "/api/v1/admin/policies/effective/{workspace_id}",
            routing::get(get_effective_policy),
        )
        .route(
            "/api/v1/admin/policies/{id}",
            routing::delete(delete_policy),
        )
        .layer(Extension(auth_mode))
        .layer(Extension(auth))
        .with_state(state)
}
