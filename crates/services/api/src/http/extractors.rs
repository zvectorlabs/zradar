//! Axum extractors for authentication

use async_trait::async_trait;
use axum::{
    RequestPartsExt,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};

use crate::auth::TokenAuth;
use crate::domain::users::{User, UserRepository};
use std::ops::Deref;
use std::sync::Arc;

/// Authenticated user extracted from JWT token
///
/// Wrapper around User to allow implementing FromRequestParts
#[derive(Debug, Clone)]
pub struct AuthenticatedUser(pub User);

impl Deref for AuthenticatedUser {
    type Target = User;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AuthenticatedUser {
    /// Get the inner User
    pub fn into_inner(self) -> User {
        self.0
    }
}

/// Extract authenticated user from JWT token in Authorization header
#[async_trait]
impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Get JwtAuth from extensions
        let jwt_auth = parts
            .extensions
            .get::<Arc<dyn TokenAuth>>()
            .ok_or_else(|| {
                tracing::error!("JwtAuth not found in request extensions");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Authentication service not configured".to_string(),
                )
            })?
            .clone();

        // Get UserRepository from extensions
        let user_storage = parts
            .extensions
            .get::<Arc<dyn UserRepository>>()
            .ok_or_else(|| {
                tracing::error!("UserRepository not found in request extensions");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "User storage not configured".to_string(),
                )
            })?
            .clone();

        // Extract Authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| {
                (
                    StatusCode::UNAUTHORIZED,
                    "Missing or invalid Authorization header".to_string(),
                )
            })?;

        // Validate JWT token
        let claims = jwt_auth.validate_token(bearer.token()).map_err(|e| {
            tracing::warn!("JWT validation failed: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                "Invalid or expired token".to_string(),
            )
        })?;

        // Load user from database
        let user = user_storage
            .get_user(claims.sub)
            .await
            .map_err(|e| {
                tracing::error!("Failed to load user: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to load user".to_string(),
                )
            })?
            .ok_or_else(|| {
                tracing::warn!(user_id = %claims.sub, "User not found");
                (StatusCode::UNAUTHORIZED, "User not found".to_string())
            })?;

        // Check if user is active
        if !user.is_active {
            return Err((
                StatusCode::FORBIDDEN,
                "User account is not active".to_string(),
            ));
        }

        Ok(AuthenticatedUser(user))
    }
}
