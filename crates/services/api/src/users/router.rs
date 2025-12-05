//! User/Auth module router

use axum::{
    Router,
    routing::{get, post},
    Extension,
};
use std::sync::Arc;

use super::{AuthService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the auth router with all endpoints
pub fn router(
    service: Arc<AuthService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route("/api/v1/auth/register", post(handlers::register))
        .route("/api/v1/auth/login", post(handlers::login))
        .route("/api/v1/auth/me", get(handlers::me))
        .route("/api/v1/auth/refresh", post(handlers::refresh))
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}

