//! Error types for the admin API

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

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ControlError {
    fn from(err: anyhow::Error) -> Self {
        ControlError::Internal(err.to_string())
    }
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ControlError::Database(e) => {
                tracing::error!("Database error: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Database error".to_string(),
                )
            }
            ControlError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ControlError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            ControlError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ControlError::Internal(msg) => {
                tracing::error!("Internal error: {}", msg);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
