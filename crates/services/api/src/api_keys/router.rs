//! API Keys module router

use axum::{
    Extension, Router,
    routing::{delete, get, post},
};
use std::sync::Arc;

use super::handlers;
use super::service::ApiKeyService;
use crate::auth::{DefaultKeyGenerator, TokenAuth};
use crate::users::UserRepository;

/// Create the API keys router with all endpoints
pub fn router(
    service: Arc<ApiKeyService<DefaultKeyGenerator>>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route(
            "/api/v1/projects/:project_id/api-keys",
            post(handlers::create_api_key),
        )
        .route(
            "/api/v1/projects/:project_id/api-keys",
            get(handlers::list_api_keys),
        )
        .route("/api/v1/api-keys/:id", get(handlers::get_api_key))
        .route(
            "/api/v1/api-keys/:id/revoke",
            post(handlers::revoke_api_key),
        )
        .route("/api/v1/api-keys/:id", delete(handlers::delete_api_key))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}
