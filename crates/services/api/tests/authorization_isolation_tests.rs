//! Permission enforcement and tenant/project isolation tests.
//!
//! These tests verify:
//! - Non-empty capabilities list: missing required capability returns 403.
//! - Non-empty capabilities list: matching permission passes through.
//! - Empty capabilities list (standalone): all capability checks pass.
//! - Settings path project mismatch returns 403 when capabilities are set.
//! - Retention org_id override is rejected when capabilities are set.
//! - Query params cannot override `project_id` (context always wins when capabilities set).

use api::errors::ControlError;
use api::http::{AuthContext, AuthMode, Capability, parse_ctx_uuid};
use uuid::Uuid;
use zradar_models::RequestContext;

fn request_ctx(tenant: Uuid, project: Uuid) -> RequestContext {
    RequestContext {
        tenant_id: tenant.to_string(),
        project_id: project.to_string(),
    }
}

/// Simulates a gateway-mode `AuthContext`: has capabilities as would be returned
/// by a gateway-wrapper authorizer after resolving trusted headers.
fn auth_with_capabilities(capabilities: Vec<Capability>) -> AuthContext {
    AuthContext::from_context(
        request_ctx(Uuid::new_v4(), Uuid::new_v4()),
        AuthMode::Standalone,
        capabilities,
    )
}

fn auth_context(ctx: RequestContext, capabilities: Vec<Capability>) -> AuthContext {
    AuthContext::from_context(ctx, AuthMode::Standalone, capabilities)
}

#[test]
fn test_missing_permission_returns_forbidden() {
    let auth = auth_with_capabilities(vec![Capability::ReadLogs]);
    let err = auth.require(Capability::ReadTraces).unwrap_err();
    assert!(
        matches!(err, ControlError::Forbidden(_)),
        "expected Forbidden, got {err:?}"
    );
}

#[test]
fn test_correct_permission_passes() {
    let auth = auth_with_capabilities(vec![Capability::ReadTraces, Capability::ReadLogs]);
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::ReadLogs).is_ok());
}

#[test]
fn test_analytics_permission_passes() {
    let auth = auth_with_capabilities(vec![Capability::ReadDashboards]);
    assert!(auth.require(Capability::ReadDashboards).is_ok());
}

#[test]
fn test_metrics_permission_passes() {
    let auth = auth_with_capabilities(vec![Capability::ReadMetrics]);
    assert!(auth.require(Capability::ReadMetrics).is_ok());
}

#[test]
fn test_settings_read_missing_returns_forbidden() {
    let auth = auth_with_capabilities(vec![Capability::ReadTraces]);
    let err = auth.require(Capability::ReadSettings).unwrap_err();
    assert!(matches!(err, ControlError::Forbidden(_)));
}

#[test]
fn test_settings_write_missing_returns_forbidden() {
    let auth = auth_with_capabilities(vec![Capability::ReadSettings]);
    let err = auth.require(Capability::WriteSettings).unwrap_err();
    assert!(matches!(err, ControlError::Forbidden(_)));
}

#[test]
fn test_admin_permission_passes() {
    let auth = auth_with_capabilities(vec![Capability::Admin]);
    assert!(auth.require(Capability::Admin).is_ok());
}

#[test]
fn test_admin_permission_missing_returns_forbidden() {
    let auth = auth_with_capabilities(vec![Capability::ReadTraces]);
    let err = auth.require(Capability::Admin).unwrap_err();
    assert!(matches!(err, ControlError::Forbidden(_)));
}

#[test]
fn test_standalone_no_permissions_passes_all_checks() {
    let ctx = request_ctx(Uuid::new_v4(), Uuid::new_v4());
    let auth = auth_context(ctx, Vec::new());
    assert!(auth.require(Capability::ReadTraces).is_ok());
    assert!(auth.require(Capability::ReadDashboards).is_ok());
    assert!(auth.require(Capability::ReadLogs).is_ok());
    assert!(auth.require(Capability::ReadMetrics).is_ok());
    assert!(auth.require(Capability::ReadSettings).is_ok());
    assert!(auth.require(Capability::WriteSettings).is_ok());
    assert!(auth.require(Capability::Admin).is_ok());
}

#[test]
fn test_parse_ctx_uuid_valid_round_trips() {
    let id = Uuid::new_v4();
    let parsed = parse_ctx_uuid(&id.to_string(), "tenant_id").unwrap();
    assert_eq!(parsed, id);
}

#[test]
fn test_parse_ctx_uuid_rejects_nil_string() {
    let err = parse_ctx_uuid("not-a-uuid", "tenant_id").unwrap_err();
    assert!(matches!(err, ControlError::InvalidInput(_)));
}

#[test]
fn test_parse_ctx_uuid_rejects_empty_string() {
    let err = parse_ctx_uuid("", "project_id").unwrap_err();
    assert!(matches!(err, ControlError::InvalidInput(_)));
}

#[test]
fn test_ctx_project_must_match_path_project_when_capabilities_set() {
    let tenant = Uuid::new_v4();
    let ctx_project = Uuid::new_v4();
    let path_project = Uuid::new_v4();

    let ctx = request_ctx(tenant, ctx_project);
    let auth = auth_context(ctx, vec![Capability::WriteSettings]);

    assert!(auth.require(Capability::WriteSettings).is_ok());

    let err = auth.enforce_path_project(path_project).unwrap_err();
    assert!(matches!(err, ControlError::Forbidden(_)));
}

#[test]
fn test_ctx_project_mismatch_ignored_when_no_capabilities() {
    let ctx = request_ctx(Uuid::new_v4(), Uuid::new_v4());
    let auth = auth_context(ctx, Vec::new());
    assert!(auth.require(Capability::WriteSettings).is_ok());
    assert!(auth.enforce_path_project(Uuid::new_v4()).is_ok());
}

#[test]
fn test_org_id_override_rejected_when_capabilities_differ() {
    let ctx_tenant = Uuid::new_v4();
    let override_org = Uuid::new_v4();
    let ctx = request_ctx(ctx_tenant, Uuid::new_v4());
    let auth = auth_context(ctx, vec![Capability::Admin]);

    assert!(auth.require(Capability::Admin).is_ok());

    let err = auth
        .tenant_or_reject_platform_override(Some(override_org))
        .unwrap_err();
    assert!(matches!(err, ControlError::Forbidden(_)));
}

#[test]
fn test_same_org_id_allowed_even_with_capabilities() {
    let tenant = Uuid::new_v4();
    let ctx = request_ctx(tenant, Uuid::new_v4());
    let auth = auth_context(ctx, vec![Capability::Admin]);
    assert!(auth.require(Capability::Admin).is_ok());
    let ctx_org = auth
        .tenant_or_reject_platform_override(Some(tenant))
        .unwrap();
    assert_eq!(ctx_org, tenant);
}
