//! User and authentication HTTP handlers

use axum::{
    extract::State,
    http::StatusCode,
    Json,
};
use std::sync::Arc;

use super::{service::AuthService, types::*};
use crate::errors::Result;
use crate::http::extractors::AuthenticatedUser;

/// Register a new user
#[utoipa::path(
    post,
    path = "/api/v1/auth/register",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User registered successfully", body = AuthResponse),
        (status = 400, description = "Invalid request"),
        (status = 409, description = "Email already exists"),
    ),
    tag = "auth"
)]
pub async fn register(
    State(service): State<Arc<AuthService>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<AuthResponse>)> {
    let response = service.register(req).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

/// Login with email and password
#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Login successful", body = AuthResponse),
        (status = 401, description = "Invalid credentials"),
    ),
    tag = "auth"
)]
pub async fn login(
    State(service): State<Arc<AuthService>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<AuthResponse>> {
    let response = service.login(req).await?;
    Ok(Json(response))
}

/// Get current user info
#[utoipa::path(
    get,
    path = "/api/v1/auth/me",
    responses(
        (status = 200, description = "Current user info", body = UserResponse),
        (status = 401, description = "Unauthorized"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "auth"
)]
pub async fn me(
    user: AuthenticatedUser,
) -> Result<Json<UserResponse>> {
    Ok(Json(user.into_inner().into()))
}

/// Refresh JWT token
#[utoipa::path(
    post,
    path = "/api/v1/auth/refresh",
    responses(
        (status = 200, description = "Token refreshed", body = RefreshResponse),
        (status = 401, description = "Invalid token"),
    ),
    security(
        ("bearer_token" = [])
    ),
    tag = "auth"
)]
pub async fn refresh(
    State(service): State<Arc<AuthService>>,
    user: AuthenticatedUser,
) -> Result<Json<RefreshResponse>> {
    // Generate a new JWT token for the authenticated user
    // AuthenticatedUser wraps User, so we pass a reference to the inner User
    let new_token = service.jwt_auth.generate_token(&user.0)?;
    
    Ok(Json(RefreshResponse {
        token: new_token,
    }))
}

