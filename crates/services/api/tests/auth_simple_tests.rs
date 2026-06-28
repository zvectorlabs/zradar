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
use zradar_models::WorkspaceId;
use zradar_traits::{AdminAuth, AdminAuthorizer};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Simple mock authorizer: validates a single hardcoded bearer token and
/// returns a fixed tenant/project context with no capabilities (standalone).
struct MockApiKeyAuthorizer {
    expected_token: &'static str,
    workspace_id: WorkspaceId,
}

impl MockApiKeyAuthorizer {
    fn new(token: &'static str) -> Self {
        Self {
            expected_token: token,
            workspace_id: uuid::Uuid::nil().into(),
        }
    }

    #[allow(dead_code)]
    fn with_context(token: &'static str, workspace_id: WorkspaceId) -> Self {
        Self {
            expected_token: token,
            workspace_id,
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
                workspace_id: self.workspace_id,
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
        let workspace_id = headers
            .get("x-workspace-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing x-workspace-id"))?;

        Ok(AdminAuth {
            context: RequestContext { workspace_id },
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
    assert_eq!(auth.context().workspace_id, Uuid::nil().into());
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
    let workspace_id = Uuid::new_v4();
    let request = Request::builder()
        .header("x-workspace-id", &workspace_id.to_string())
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
    assert_eq!(auth.context().workspace_id, workspace_id.into());
}

#[tokio::test]
async fn test_gateway_injects_capabilities_from_trusted_headers() {
    let request = Request::builder()
        .header("x-workspace-id", Uuid::nil().to_string())
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
    workspace_id: WorkspaceId,
    capabilities: Vec<Capability>,
) -> AuthContext {
    AuthContext::from_context(
        RequestContext { workspace_id },
        AuthMode::Standalone,
        capabilities,
    )
}

#[test]
fn test_capability_require_empty_list_always_passes() {
    let auth = auth_context_with_capabilities(Uuid::nil().into(), Vec::new());
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::Admin).is_ok());
}

#[test]
fn test_capability_require_grants_present_capability() {
    let auth = auth_context_with_capabilities(
        Uuid::nil().into(),
        vec![Capability::ReadTraces, Capability::ReadDashboards],
    );
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::ReadDashboards).is_ok());
}

#[test]
fn test_capability_require_denies_absent_capability() {
    let auth = auth_context_with_capabilities(Uuid::nil().into(), vec![Capability::ReadTraces]);
    let result = auth.require(Capability::Admin);
    assert!(
        matches!(result, Err(ControlError::Forbidden(_))),
        "Expected Forbidden for missing capability"
    );
}

#[test]
fn test_enforce_path_project_no_capabilities_is_noop() {
    let workspace_id = Uuid::new_v4();
    let auth = auth_context_with_capabilities(Uuid::nil().into(), Vec::new());
    assert!(auth.enforce_path_workspace(workspace_id).is_ok());
}

#[test]
fn test_enforce_path_project_with_capabilities_rejects_mismatch() {
    let workspace_id = Uuid::new_v4();
    let other = Uuid::new_v4();
    let auth = auth_context_with_capabilities(workspace_id.into(), vec![Capability::ReadTraces]);
    let result = auth.enforce_path_workspace(other);
    assert!(
        matches!(result, Err(ControlError::Forbidden(_))),
        "Expected Forbidden when path project doesn't match authenticated project"
    );
}

#[test]
fn test_tenant_override_allowed_without_capabilities() {
    let workspace_id = Uuid::new_v4();
    let override_workspace = Uuid::new_v4();
    let auth = auth_context_with_capabilities(workspace_id.into(), Vec::new());
    let result = auth.workspace_or_standalone_override(Some(override_workspace));
    assert_eq!(result.unwrap(), override_workspace);
}

#[test]
fn test_tenant_override_ignored_with_capabilities() {
    let workspace_id = Uuid::new_v4();
    let override_workspace = Uuid::new_v4();
    let auth = auth_context_with_capabilities(workspace_id.into(), vec![Capability::Admin]);
    let result = auth.workspace_or_standalone_override(Some(override_workspace));
    assert_eq!(result.unwrap(), workspace_id);
}
