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
//! 400 if either is missing or empty. Gateway-specific authorization headers are
//! translated into zradar-native capabilities at this boundary.

use axum::{
    Extension,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

use crate::errors::{ControlError, Result as ApiResult};

/// Authentication mode injected as an axum extension by the router.
///
/// The server reads this from `config.toml` (`auth.mode`) and sets it once at
/// startup; handlers do not need to inspect it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Standalone,
    Platform,
}

/// zradar-native authorization capabilities.
///
/// HTTP handlers depend on these capabilities instead of gateway-specific
/// permission strings or transport headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    ReadTraces,
    ReadDashboards,
    ReadLogs,
    ReadMetrics,
    ReadSettings,
    WriteSettings,
    Admin,
}

/// Axum extractor that resolves the `RequestContext` and configured auth mode
/// from the Bearer token.
///
/// Requires both `Extension(Arc<dyn Authenticator>)` and `Extension(AuthMode)`
/// to be present on the router.
pub struct AuthContext {
    /// Request-scoped tenant/project/user context.
    ctx: RequestContext,
    /// Auth mode read from config and injected by the router.
    mode: AuthMode,
    /// zradar-native capabilities resolved by the auth adapter.
    capabilities: Vec<Capability>,
}

impl AuthContext {
    /// Creates an auth context from resolved request context and mode.
    ///
    /// This is primarily useful for focused tests; production requests should
    /// use the extractor implementation.
    #[doc(hidden)]
    pub fn from_context(
        ctx: RequestContext,
        mode: AuthMode,
        capabilities: Vec<Capability>,
    ) -> Self {
        Self {
            ctx,
            mode,
            capabilities,
        }
    }

    /// Returns the request context resolved by authentication.
    pub fn context(&self) -> &RequestContext {
        &self.ctx
    }

    /// Returns the auth mode resolved from server configuration.
    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Returns true when the request was authenticated through platform mode.
    pub fn is_platform(&self) -> bool {
        self.mode == AuthMode::Platform
    }

    /// Enforces a zradar capability in platform mode.
    ///
    /// Standalone callers are authorized by API key scope and always pass here.
    pub fn require(&self, capability: Capability) -> ApiResult<()> {
        if self.mode == AuthMode::Standalone {
            return Ok(());
        }

        if self.capabilities.contains(&capability) {
            Ok(())
        } else {
            Err(ControlError::Forbidden(format!(
                "missing capability: {capability:?}"
            )))
        }
    }

    /// Parses the authenticated tenant ID.
    pub fn tenant_uuid(&self) -> ApiResult<Uuid> {
        parse_ctx_uuid(&self.ctx.tenant_id, "tenant_id")
    }

    /// Parses the authenticated project ID.
    pub fn project_uuid(&self) -> ApiResult<Uuid> {
        parse_ctx_uuid(&self.ctx.project_id, "project_id")
    }

    /// Returns the authenticated project ID string.
    pub fn project_id(&self) -> &str {
        &self.ctx.project_id
    }

    /// Enforces that a path project matches the authenticated project in platform mode.
    pub fn enforce_path_project(&self, path_project: Uuid) -> ApiResult<()> {
        if !self.is_platform() {
            return Ok(());
        }

        if self.project_uuid()? != path_project {
            return Err(ControlError::Forbidden(
                "path project_id does not match authenticated project".to_string(),
            ));
        }

        Ok(())
    }

    /// Returns the tenant unless standalone mode explicitly supplies an org override.
    ///
    /// Platform mode ignores the caller-provided override and always uses the
    /// trusted tenant from context.
    pub fn tenant_or_standalone_override(&self, requested_org: Option<Uuid>) -> ApiResult<Uuid> {
        let tenant_id = self.tenant_uuid()?;
        if self.is_platform() {
            Ok(tenant_id)
        } else {
            Ok(requested_org.unwrap_or(tenant_id))
        }
    }

    /// Returns the tenant or rejects a platform org override that differs from it.
    pub fn tenant_or_reject_platform_override(
        &self,
        requested_org: Option<Uuid>,
    ) -> ApiResult<Uuid> {
        let tenant_id = self.tenant_uuid()?;
        if !self.is_platform() {
            return Ok(requested_org.unwrap_or(tenant_id));
        }

        if let Some(requested) = requested_org
            && requested != tenant_id
        {
            return Err(ControlError::Forbidden(
                "org_id override not allowed in platform mode".to_string(),
            ));
        }

        Ok(tenant_id)
    }

    /// Returns tenant/project filters scoped for audit reads.
    ///
    /// Standalone keeps caller-provided filters. Platform ignores caller filters
    /// and uses the trusted tenant/project from context.
    pub fn audit_scope(
        &self,
        requested_org: Option<Uuid>,
        requested_project: Option<Uuid>,
    ) -> ApiResult<(Option<Uuid>, Option<Uuid>)> {
        if self.is_platform() {
            Ok((Some(self.tenant_uuid()?), Some(self.project_uuid()?)))
        } else {
            Ok((requested_org, requested_project))
        }
    }
}

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

        let Extension(mode): Extension<AuthMode> = Extension::from_request_parts(parts, state)
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

        let (ctx, capabilities) = match mode {
            AuthMode::Standalone => (build_standalone_context(base_ctx, parts), Vec::new()),
            AuthMode::Platform => {
                let platform = PlatformHeaders::from_request(parts)?;
                (platform.context, platform.capabilities)
            }
        };

        Ok(AuthContext {
            ctx,
            mode,
            capabilities,
        })
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

/// Trusted-header adapter for gateway-managed deployments.
///
/// Transport headers are intentionally kept private to this boundary. The rest
/// of zradar sees only [`RequestContext`] plus zradar-native [`Capability`] values.
struct PlatformHeaders {
    context: RequestContext,
    capabilities: Vec<Capability>,
}

impl PlatformHeaders {
    /// Builds neutral request context and capabilities from trusted gateway headers.
    fn from_request(parts: &Parts) -> Result<Self, AuthError> {
        let tenant_id = header_str(parts, "x-tenant-id")
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| AuthError("Platform mode requires tenant context header".to_string()))?;

        let project_id = header_str(parts, "x-project-id")
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                AuthError("Platform mode requires project context header".to_string())
            })?;

        let capabilities = header_str(parts, "x-zradar-capabilities")
            .map(parse_capabilities)
            .unwrap_or_default();

        Ok(Self {
            context: RequestContext {
                tenant_id,
                project_id,
            },
            capabilities,
        })
    }
}

/// Parses zradar-native capability wire values.
fn parse_capabilities(header: &str) -> Vec<Capability> {
    header
        .split(',')
        .filter_map(|p| match p.trim() {
            "read_traces" => Some(Capability::ReadTraces),
            "read_dashboards" => Some(Capability::ReadDashboards),
            "read_logs" => Some(Capability::ReadLogs),
            "read_metrics" => Some(Capability::ReadMetrics),
            "read_settings" => Some(Capability::ReadSettings),
            "write_settings" => Some(Capability::WriteSettings),
            "admin" => Some(Capability::Admin),
            _ => None,
        })
        .collect()
}

/// Reads a header value as a `&str` from the request parts.
fn header_str<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}

/// Parses a UUID string from trusted request context.
pub fn parse_ctx_uuid(value: &str, field: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(value).map_err(|_| {
        ControlError::InvalidInput(format!("invalid {field} in request context: {value}"))
    })
}
