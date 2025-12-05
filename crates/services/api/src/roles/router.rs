//! Roles module router

use axum::{
    Router,
    routing::{get, post, put, delete},
    Extension,
};
use std::sync::Arc;

use super::{RoleService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the roles router with all endpoints
pub fn router(
    service: Arc<RoleService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route("/api/v1/organizations/:org_id/roles", post(handlers::create_role))
        .route("/api/v1/organizations/:org_id/roles", get(handlers::list_roles))
        .route("/api/v1/roles/:role_id", get(handlers::get_custom_role))
        .route("/api/v1/roles/:role_id", put(handlers::update_custom_role))
        .route("/api/v1/roles/:role_id", delete(handlers::delete_custom_role))
        .route("/api/v1/permissions", get(handlers::list_permissions))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}

