//! Axum extractor that validates the `Authorization: Bearer` header and builds
//! the `RequestContext` according to the configured auth mode.
//!
//! # Standalone mode (default)
//! Token is validated against the static API key map. `tenant_id` and `project_id`
//! come from the key entry; optional `x-tenant-id` / `x-project-id` headers are
//! accepted as overrides (legacy behaviour for intra-org routing).
//!
//! # Platform mode (Agnitiv gateway)
//! Token is validated as the gateway service credential. `x-tenant-id` and
//! `x-project-id` are **required** trusted headers — the request is rejected with
//! 400 if either is missing or empty. Optional `x-user-id`, `x-org-slug`, and
//! `x-permissions` are parsed into the context for audit and M02 enforcement.

use axum::{
    Extension,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

/// Authentication mode injected as an axum extension by the router.
///
/// The server reads this from `config.toml` (`auth.mode`) and sets it once at
/// startup; handlers do not need to inspect it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Standalone,
    Platform,
}

/// Axum extractor that resolves the `RequestContext` from the Bearer token.
///
/// Requires both `Extension(Arc<dyn Authenticator>)` and `Extension(AuthMode)`
/// to be present on the router.
pub struct AuthContext(pub RequestContext);

#[derive(Debug)]
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

        let Extension(mode): Extension<AuthMode> =
            Extension::from_request_parts(parts, state)
                .await
                .map_err(|_| AuthError("Auth mode not configured".to_string()))?;

        let token = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(|| AuthError("Missing or invalid Authorization header".to_string()))?;

        let base_ctx = auth
            .authenticate(token)
            .await
            .map_err(|_| AuthError("Invalid credentials".to_string()))?;

        let ctx = match mode {
            AuthMode::Standalone => build_standalone_context(base_ctx, parts),
            AuthMode::Platform => build_platform_context(parts)?,
        };

        Ok(AuthContext(ctx))
    }
}

/// Builds context for standalone mode.
///
/// `base_ctx` holds the tenant/project bound to the API key.
/// Optional `x-tenant-id` and `x-project-id` headers override those values
/// for intra-org routing (existing behaviour preserved).
fn build_standalone_context(mut base_ctx: RequestContext, parts: &Parts) -> RequestContext {
    if let Some(val) = header_str(parts, "x-tenant-id") {
        base_ctx.tenant_id = val.to_string();
    }
    if let Some(val) = header_str(parts, "x-project-id") {
        base_ctx.project_id = val.to_string();
    }
    base_ctx
}

/// Builds context for platform mode.
///
/// `x-tenant-id` and `x-project-id` are **required**; the request is rejected
/// with 400 (mapped to 401 via `AuthError`) if either is absent or empty.
/// Additional trusted headers are parsed if present.
///
/// The authenticator has already confirmed the gateway service token is valid
/// before this function is called, so we trust all gateway-provided headers.
fn build_platform_context(parts: &Parts) -> Result<RequestContext, AuthError> {
    let tenant_id = header_str(parts, "x-tenant-id")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            AuthError(
                "Platform mode requires x-tenant-id header (Agnitiv org_id)".to_string(),
            )
        })?;

    let project_id = header_str(parts, "x-project-id")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            AuthError(
                "Platform mode requires x-project-id header (validated project ID)".to_string(),
            )
        })?;

    let user_id = header_str(parts, "x-user-id")
        .unwrap_or("")
        .to_string();

    let org_slug = header_str(parts, "x-org-slug")
        .unwrap_or("")
        .to_string();

    let permissions = header_str(parts, "x-permissions")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    Ok(RequestContext {
        tenant_id,
        project_id,
        user_id,
        org_slug,
        permissions,
    })
}

/// Reads a header value as a `&str` from the request parts.
fn header_str<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}
