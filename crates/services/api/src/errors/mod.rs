//! Error types for zradar-control

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ControlError>;

#[derive(Error, Debug)]
pub enum ControlError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Plugin error: {0}")]
    Plugin(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("JWT error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("Password hashing error")]
    PasswordHash,

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ControlError {
    fn from(err: anyhow::Error) -> Self {
        ControlError::Plugin(err.to_string())
    }
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ControlError::Database(ref e) => {
                tracing::error!("Database error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error")
            }
            ControlError::Plugin(ref msg) => {
                tracing::error!("Plugin error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error")
            }
            ControlError::NotFound(ref msg) => (StatusCode::NOT_FOUND, msg.as_str()),
            ControlError::Unauthorized(ref msg) => (StatusCode::UNAUTHORIZED, msg.as_str()),
            ControlError::Forbidden(ref msg) => (StatusCode::FORBIDDEN, msg.as_str()),
            ControlError::InvalidInput(ref msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
            ControlError::Conflict(ref msg) => (StatusCode::CONFLICT, msg.as_str()),
            ControlError::AuthenticationFailed(ref msg) => (StatusCode::UNAUTHORIZED, msg.as_str()),
            ControlError::Jwt(_) => (StatusCode::UNAUTHORIZED, "Invalid token"),
            ControlError::PasswordHash => {
                (StatusCode::INTERNAL_SERVER_ERROR, "Password hashing error")
            }
            ControlError::Internal(ref msg) => {
                tracing::error!("Internal error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}
