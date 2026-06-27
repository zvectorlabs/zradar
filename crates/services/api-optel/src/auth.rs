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
/// Tenant and project are bound to the authenticated token by default.
///
/// When `allow_test_header_context` is true, `x-tenant-id` and `x-project-id`
/// are applied after bearer-token validation. This mode exists only for
/// functional/E2E tests that need to simulate many API keys with one static key.
pub async fn authenticate_grpc<T>(
    auth: &Option<Arc<dyn Authenticator>>,
    request: &Request<T>,
    allow_test_header_context: bool,
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

    let mut context = authenticator.authenticate(token).await.map_err(|e| {
        tracing::warn!("API key validation failed: {}", e);
        Status::unauthenticated("Invalid API key")
    })?;

    if allow_test_header_context {
        if let Some(tenant_id) = metadata
            .get("x-tenant-id")
            .and_then(|value| value.to_str().ok())
        {
            context.tenant_id = tenant_id.to_string();
        }
        if let Some(project_id) = metadata
            .get("x-project-id")
            .and_then(|value| value.to_str().ok())
        {
            context.project_id = project_id.to_string();
        }
    }

    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockAuthenticator;

    #[tonic::async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
            if token != "valid" {
                anyhow::bail!("invalid token");
            }
            Ok(RequestContext {
                tenant_id: "auth-tenant".to_string(),
                project_id: "auth-project".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn test_authenticate_grpc_ignores_header_context_by_default() {
        let auth: Option<Arc<dyn Authenticator>> = Some(Arc::new(MockAuthenticator));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer valid".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-tenant-id", "header-tenant".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-project-id", "header-project".parse().unwrap());

        let context = authenticate_grpc(&auth, &request, false).await.unwrap();

        assert_eq!(context.tenant_id, "auth-tenant");
        assert_eq!(context.project_id, "auth-project");
    }

    #[tokio::test]
    async fn test_authenticate_grpc_applies_header_context_in_test_mode() {
        let auth: Option<Arc<dyn Authenticator>> = Some(Arc::new(MockAuthenticator));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer valid".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-tenant-id", "header-tenant".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-project-id", "header-project".parse().unwrap());

        let context = authenticate_grpc(&auth, &request, true).await.unwrap();

        assert_eq!(context.tenant_id, "header-tenant");
        assert_eq!(context.project_id, "header-project");
    }
}
