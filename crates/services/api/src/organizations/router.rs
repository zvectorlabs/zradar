//! Organization module router

use axum::{
    Router,
    routing::{get, post, put, patch, delete},
    Extension,
};
use std::sync::Arc;

use super::{OrganizationService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the organization router with all endpoints
pub fn router(
    service: Arc<OrganizationService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route("/api/v1/organizations", post(handlers::create_organization))
        .route("/api/v1/organizations", get(handlers::list_organizations))
        .route("/api/v1/organizations/:id", get(handlers::get_organization))
        .route("/api/v1/organizations/:id", put(handlers::update_organization))
        .route("/api/v1/organizations/:id", patch(handlers::update_organization))
        .route("/api/v1/organizations/:id", delete(handlers::delete_organization))
        .route("/api/v1/organizations/:id/members", post(handlers::add_organization_member))
        .route("/api/v1/organizations/:id/members", get(handlers::list_organization_members))
        .route("/api/v1/organizations/:id/members/:user_id", delete(handlers::remove_organization_member))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}

