//! Scores module router

use axum::{
    Extension, Router,
    routing::{delete, get, post},
};
use std::sync::Arc;

use super::{ScoresService, handlers};
use crate::auth::TokenAuth;
use crate::users::UserRepository;

/// Create the scores router with all endpoints
pub fn router(
    service: Arc<ScoresService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    Router::new()
        .route(
            "/api/v1/projects/:project_id/scores",
            post(handlers::create_score),
        )
        .route(
            "/api/v1/projects/:project_id/traces/:trace_id/scores",
            get(handlers::get_trace_scores),
        )
        .route(
            "/api/v1/projects/:project_id/traces/:trace_id/scores/summary",
            get(handlers::get_trace_score_summary),
        )
        .route(
            "/api/v1/projects/:project_id/sessions/:session_id/scores",
            get(handlers::get_session_scores),
        )
        .route(
            "/api/v1/projects/:project_id/scores/:score_id",
            get(handlers::get_score_by_id),
        )
        .route(
            "/api/v1/projects/:project_id/scores/:score_id",
            delete(handlers::delete_score),
        )
        .with_state(service)
        .layer(Extension(jwt_auth))
        .layer(Extension(user_storage))
}
