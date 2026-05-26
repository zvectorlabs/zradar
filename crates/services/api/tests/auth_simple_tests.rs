//! Tests for the HTTP auth extractor.
//!
//! Covers standalone API-key mode (via `ApiKeyAdminAuthorizer`-style mocks)
//! and capability injection (simulating what a gateway wrapper produces).
//! Gateway-specific header parsing is tested in the wrapper crate.

use api::{
    errors::ControlError,
    http::{AuthContext, AuthMode, Capability},
};
use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::{HeaderMap, Request, header::AUTHORIZATION};
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::RequestContext;
use zradar_traits::{AdminAuth, AdminAuthorizer};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Simple mock authorizer: validates a single hardcoded bearer token and
/// returns a fixed tenant/project context with no capabilities (standalone).
struct MockApiKeyAuthorizer {
    expected_token: &'static str,
    tenant_id: String,
    project_id: String,
}

impl MockApiKeyAuthorizer {
    fn new(token: &'static str) -> Self {
        Self {
            expected_token: token,
            tenant_id: Uuid::nil().to_string(),
            project_id: Uuid::nil().to_string(),
        }
    }

    fn with_context(token: &'static str, tenant_id: &str, project_id: &str) -> Self {
        Self {
            expected_token: token,
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
        }
    }
}

#[async_trait]
impl AdminAuthorizer for MockApiKeyAuthorizer {
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AdminAuth> {
        let bearer = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(|| anyhow::anyhow!("Missing Authorization header"))?;

        if bearer != self.expected_token {
            anyhow::bail!("invalid token");
        }

        Ok(AdminAuth {
            context: RequestContext {
                tenant_id: self.tenant_id.clone(),
                project_id: self.project_id.clone(),
            },
            capability_keys: Vec::new(),
        })
    }
}

/// Mock authorizer that reads trusted context headers (tenant/project) from the
/// request and returns a configured set of capability keys. Simulates what a
/// gateway wrapper authorizer produces.
struct MockGatewayAuthorizer {
    capability_keys: Vec<String>,
}

impl MockGatewayAuthorizer {
    fn new(capability_keys: Vec<&'static str>) -> Self {
        Self {
            capability_keys: capability_keys.into_iter().map(String::from).collect(),
        }
    }
}

#[async_trait]
impl AdminAuthorizer for MockGatewayAuthorizer {
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AdminAuth> {
        let tenant_id = headers
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing x-tenant-id"))?
            .to_string();
        let project_id = headers
            .get("x-project-id")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing x-project-id"))?
            .to_string();

        Ok(AdminAuth {
            context: RequestContext {
                tenant_id,
                project_id,
            },
            capability_keys: self.capability_keys.clone(),
        })
    }
}

fn make_parts(
    req: Request<()>,
    auth: Arc<dyn AdminAuthorizer>,
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
    let mut parts = make_parts(
        request,
        Arc::new(MockApiKeyAuthorizer::new("valid-token")),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let auth = result.unwrap();
    assert_eq!(auth.mode(), AuthMode::Standalone);
    assert_eq!(auth.context().tenant_id, Uuid::nil().to_string());
    assert_eq!(auth.context().project_id, Uuid::nil().to_string());
}

#[tokio::test]
async fn test_standalone_rejects_invalid_token() {
    let request = Request::builder()
        .header("authorization", "Bearer bad-key")
        .body(())
        .unwrap();
    let mut parts = make_parts(
        request,
        Arc::new(MockApiKeyAuthorizer::new("valid-token")),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err(), "Expected rejection for invalid token");
}

#[tokio::test]
async fn test_standalone_rejects_missing_authorization_header() {
    let request = Request::builder().body(()).unwrap();
    let mut parts = make_parts(
        request,
        Arc::new(MockApiKeyAuthorizer::new("valid-token")),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(
        result.is_err(),
        "Expected rejection when Authorization header is absent"
    );
}

// ---------------------------------------------------------------------------
// Gateway mode — trusted header extraction and capability injection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gateway_injects_tenant_project_from_trusted_headers() {
    let tenant_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let request = Request::builder()
        .header("x-tenant-id", &tenant_id)
        .header("x-project-id", &project_id)
        .body(())
        .unwrap();
    let mut parts = make_parts(
        request,
        Arc::new(MockGatewayAuthorizer::new(vec!["read_traces"])),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let auth = result.unwrap();
    assert_eq!(auth.context().tenant_id, tenant_id);
    assert_eq!(auth.context().project_id, project_id);
}

#[tokio::test]
async fn test_gateway_injects_capabilities_from_trusted_headers() {
    let request = Request::builder()
        .header("x-tenant-id", Uuid::nil().to_string())
        .header("x-project-id", Uuid::nil().to_string())
        .body(())
        .unwrap();
    let mut parts = make_parts(
        request,
        Arc::new(MockGatewayAuthorizer::new(vec!["read_traces", "read_logs"])),
        AuthMode::Standalone,
    );

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let auth = result.unwrap();
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::ReadLogs).is_ok());
    assert!(matches!(
        auth.require(Capability::Admin),
        Err(ControlError::Forbidden(_))
    ));
}

// ---------------------------------------------------------------------------
// Capability enforcement tests
// ---------------------------------------------------------------------------

fn auth_context_with_capabilities(
    tenant_id: &str,
    project_id: &str,
    capabilities: Vec<Capability>,
) -> AuthContext {
    AuthContext::from_context(
        RequestContext {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
        },
        AuthMode::Standalone,
        capabilities,
    )
}

#[test]
fn test_capability_require_empty_list_always_passes() {
    let auth = auth_context_with_capabilities(
        &Uuid::nil().to_string(),
        &Uuid::nil().to_string(),
        Vec::new(),
    );
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::Admin).is_ok());
}

#[test]
fn test_capability_require_grants_present_capability() {
    let auth = auth_context_with_capabilities(
        &Uuid::nil().to_string(),
        &Uuid::nil().to_string(),
        vec![Capability::ReadTraces, Capability::ReadDashboards],
    );
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::ReadDashboards).is_ok());
}

#[test]
fn test_capability_require_denies_absent_capability() {
    let auth = auth_context_with_capabilities(
        &Uuid::nil().to_string(),
        &Uuid::nil().to_string(),
        vec![Capability::ReadTraces],
    );
    let result = auth.require(Capability::Admin);
    assert!(
        matches!(result, Err(ControlError::Forbidden(_))),
        "Expected Forbidden for missing capability"
    );
}

#[test]
fn test_enforce_path_project_no_capabilities_is_noop() {
    let project_id = Uuid::new_v4();
    let auth = auth_context_with_capabilities(
        &Uuid::nil().to_string(),
        &Uuid::nil().to_string(),
        Vec::new(),
    );
    assert!(auth.enforce_path_project(project_id).is_ok());
}

#[test]
fn test_enforce_path_project_with_capabilities_rejects_mismatch() {
    let project_id = Uuid::new_v4();
    let other = Uuid::new_v4();
    let auth = auth_context_with_capabilities(
        &Uuid::nil().to_string(),
        &project_id.to_string(),
        vec![Capability::ReadTraces],
    );
    let result = auth.enforce_path_project(other);
    assert!(
        matches!(result, Err(ControlError::Forbidden(_))),
        "Expected Forbidden when path project doesn't match authenticated project"
    );
}

#[test]
fn test_tenant_override_allowed_without_capabilities() {
    let tenant_id = Uuid::new_v4();
    let override_org = Uuid::new_v4();
    let auth = auth_context_with_capabilities(
        &tenant_id.to_string(),
        &Uuid::nil().to_string(),
        Vec::new(),
    );
    let result = auth.tenant_or_standalone_override(Some(override_org));
    assert_eq!(result.unwrap(), override_org);
}

#[test]
fn test_tenant_override_ignored_with_capabilities() {
    let tenant_id = Uuid::new_v4();
    let override_org = Uuid::new_v4();
    let auth = auth_context_with_capabilities(
        &tenant_id.to_string(),
        &Uuid::nil().to_string(),
        vec![Capability::Admin],
    );
    let result = auth.tenant_or_standalone_override(Some(override_org));
    assert_eq!(result.unwrap(), tenant_id);
}
