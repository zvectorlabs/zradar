//! Axum extractor that validates the `Authorization: Bearer <key>` header
//! using the `Authenticator` trait.

use axum::{
    Extension,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

/// Axum extractor that resolves the `RequestContext` from the Bearer token.
///
/// Requires `Extension(Arc<dyn Authenticator>)` to be present on the router.
pub struct AuthContext(pub RequestContext);

pub struct AuthError(pub String);

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (StatusCode::UNAUTHORIZED, self.0).into_response()
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(auth): Extension<Arc<dyn Authenticator>> =
            Extension::from_request_parts(parts, state)
                .await
                .map_err(|_| AuthError("Authenticator not configured".to_string()))?;

        let token = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(|| AuthError("Missing or invalid Authorization header".to_string()))?;

        let mut ctx = auth
            .authenticate(token)
            .await
            .map_err(|_| AuthError("Invalid API key".to_string()))?;

        // Allow header overrides for tenant/project isolation
        if let Some(val) = parts
            .headers
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
        {
            ctx.tenant_id = val.to_string();
        }
        if let Some(val) = parts
            .headers
            .get("x-project-id")
            .and_then(|v| v.to_str().ok())
        {
            ctx.project_id = val.to_string();
        }

        Ok(AuthContext(ctx))
    }
}
