//! Authentication helper for OTLP gRPC services.
//!
//! Extracts the `Bearer <token>` from gRPC request metadata and delegates
//! to the `Authenticator` trait.

use std::sync::Arc;
use tonic::{Request, Status};
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

/// Extract and validate the API key from a gRPC request.
///
/// Returns `RequestContext::default()` when `auth` is `None` (auth disabled).
///
/// After authentication, `x-tenant-id` and `x-project-id` metadata headers
/// override the values from the API key config. This allows tests (and
/// clients) to target a specific tenant/project without provisioning
/// separate API keys.
pub async fn authenticate_grpc<T>(
    auth: &Option<Arc<dyn Authenticator>>,
    request: &Request<T>,
) -> Result<RequestContext, Status> {
    let Some(authenticator) = auth else {
        return Ok(RequestContext::default());
    };

    let metadata = request.metadata();
    let auth_header = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("Expected: Bearer <api-key>"))?;

    let mut ctx = authenticator.authenticate(token).await.map_err(|e| {
        tracing::warn!("API key validation failed: {}", e);
        Status::unauthenticated("Invalid API key")
    })?;

    // Allow header overrides for tenant/project isolation
    if let Some(val) = metadata.get("x-tenant-id").and_then(|v| v.to_str().ok()) {
        ctx.tenant_id = val.to_string();
    }
    if let Some(val) = metadata.get("x-project-id").and_then(|v| v.to_str().ok()) {
        ctx.project_id = val.to_string();
    }

    Ok(ctx)
}
