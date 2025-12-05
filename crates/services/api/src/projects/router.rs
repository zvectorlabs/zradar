//! Project module router

use axum::{
    Router,
    routing::{get, post, put, patch, delete},
    Extension,
};
use std::sync::Arc;

use super::{ProjectService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the project router with all endpoints
pub fn router(
    service: Arc<ProjectService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route("/api/v1/organizations/:org_id/projects", post(handlers::create_project))
        .route("/api/v1/organizations/:org_id/projects", get(handlers::list_projects))
        .route("/api/v1/projects/:id", get(handlers::get_project))
        .route("/api/v1/projects/:id", put(handlers::update_project))
        .route("/api/v1/projects/:id", patch(handlers::update_project))
        .route("/api/v1/projects/:id", delete(handlers::delete_project))
        .route("/api/v1/projects/:id/members", post(handlers::add_project_member))
        .route("/api/v1/projects/:id/members", get(handlers::list_project_members))
        .route("/api/v1/projects/:id/members/:user_id", delete(handlers::remove_project_member))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}

