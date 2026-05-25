use api::http::{AuthContext, AuthMode};
use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::Request;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

struct StandaloneAuthenticator;

#[async_trait]
impl Authenticator for StandaloneAuthenticator {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
        if token != "valid-token" {
            anyhow::bail!("invalid token");
        }
        Ok(RequestContext {
            tenant_id: Uuid::nil().to_string(),
            project_id: Uuid::nil().to_string(),
            ..Default::default()
        })
    }
}

/// Platform authenticator: accepts "gw-token", rejects everything else.
struct PlatformTokenAuthenticator;

#[async_trait]
impl Authenticator for PlatformTokenAuthenticator {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
        if token != "gw-token" {
            anyhow::bail!("invalid gateway service token");
        }
        // Return default — real context is built from trusted headers
        Ok(RequestContext::default())
    }
}

fn make_parts_with_extensions(
    req: Request<()>,
    auth: Arc<dyn Authenticator>,
    mode: AuthMode,
) -> axum::http::request::Parts {
    let (mut parts, _) = req.into_parts();
    parts.extensions.insert(auth);
    parts.extensions.insert(mode);
    parts
}

// ---------------------------------------------------------------------------
// Standalone mode tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standalone_accepts_valid_bearer_token() {
    let request = Request::builder()
        .header("authorization", "Bearer valid-token")
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(StandaloneAuthenticator),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let AuthContext(ctx) = result.unwrap();
    assert_eq!(ctx.tenant_id, Uuid::nil().to_string());
    assert_eq!(ctx.project_id, Uuid::nil().to_string());
}

#[tokio::test]
async fn test_standalone_applies_tenant_and_project_header_overrides() {
    let tenant_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let request = Request::builder()
        .header("authorization", "Bearer valid-token")
        .header("x-tenant-id", &tenant_id)
        .header("x-project-id", &project_id)
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(StandaloneAuthenticator),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let AuthContext(ctx) = result.unwrap();
    assert_eq!(ctx.tenant_id, tenant_id);
    assert_eq!(ctx.project_id, project_id);
}

#[tokio::test]
async fn test_standalone_rejects_invalid_token() {
    let request = Request::builder()
        .header("authorization", "Bearer bad-key")
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(StandaloneAuthenticator),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection for invalid token");
}

#[tokio::test]
async fn test_standalone_rejects_missing_authorization_header() {
    let request = Request::builder().body(()).unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(StandaloneAuthenticator),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection when Authorization header is absent");
}

// ---------------------------------------------------------------------------
// Platform mode tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_platform_accepts_valid_gateway_token_with_required_headers() {
    let tenant_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let user_id = Uuid::new_v4().to_string();
    let request = Request::builder()
        .header("authorization", "Bearer gw-token")
        .header("x-tenant-id", &tenant_id)
        .header("x-project-id", &project_id)
        .header("x-user-id", &user_id)
        .header("x-org-slug", "acme")
        .header("x-permissions", "zradar:traces:read,zradar:dashboards:read")
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok(), "Expected success with valid gateway token and required headers");
    let AuthContext(ctx) = result.unwrap();
    assert_eq!(ctx.tenant_id, tenant_id);
    assert_eq!(ctx.project_id, project_id);
    assert_eq!(ctx.user_id, user_id);
    assert_eq!(ctx.org_slug, "acme");
    assert_eq!(ctx.permissions, vec!["zradar:traces:read", "zradar:dashboards:read"]);
}

#[tokio::test]
async fn test_platform_rejects_bad_gateway_token() {
    let request = Request::builder()
        .header("authorization", "Bearer wrong-token")
        .header("x-tenant-id", &Uuid::new_v4().to_string())
        .header("x-project-id", &Uuid::new_v4().to_string())
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection for bad gateway token");
}

#[tokio::test]
async fn test_platform_rejects_missing_x_tenant_id() {
    let request = Request::builder()
        .header("authorization", "Bearer gw-token")
        // x-tenant-id intentionally absent
        .header("x-project-id", &Uuid::new_v4().to_string())
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection when x-tenant-id is missing");
}

#[tokio::test]
async fn test_platform_rejects_missing_x_project_id() {
    let request = Request::builder()
        .header("authorization", "Bearer gw-token")
        .header("x-tenant-id", &Uuid::new_v4().to_string())
        // x-project-id intentionally absent
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection when x-project-id is missing");
}

#[tokio::test]
async fn test_platform_rejects_empty_x_tenant_id() {
    let request = Request::builder()
        .header("authorization", "Bearer gw-token")
        .header("x-tenant-id", "")
        .header("x-project-id", &Uuid::new_v4().to_string())
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection for empty x-tenant-id");
}

#[tokio::test]
async fn test_platform_accepts_no_permissions_header() {
    let tenant_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let request = Request::builder()
        .header("authorization", "Bearer gw-token")
        .header("x-tenant-id", &tenant_id)
        .header("x-project-id", &project_id)
        // No x-permissions header
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let AuthContext(ctx) = result.unwrap();
    assert!(ctx.permissions.is_empty());
}

// ---------------------------------------------------------------------------
// Contract: browser auth must NOT leak through in platform mode
// (i.e., a caller sending x-tenant-id must also send valid gateway token)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_platform_spoofed_context_headers_rejected_without_valid_gateway_token() {
    let request = Request::builder()
        .header("authorization", "Bearer user-jwt-not-gateway-token")
        .header("x-tenant-id", &Uuid::new_v4().to_string())
        .header("x-project-id", &Uuid::new_v4().to_string())
        .body(())
        .unwrap();
    let mut parts = make_parts_with_extensions(
        request,
        Arc::new(PlatformTokenAuthenticator),
        AuthMode::Platform,
    );

    // Even though the context headers are present, the invalid service token
    // must be rejected before any context is parsed.
    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection: user JWT must not bypass platform gateway token check");
}
