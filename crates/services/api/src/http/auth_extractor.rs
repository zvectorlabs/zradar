//! Axum extractor that resolves the request authorization and builds `AuthContext`.
//!
//! Query routes inject `Arc<dyn QueryAuthorizer>`; admin routes inject
//! `Arc<dyn AdminAuthorizer>`. Both share the same extractor and `AuthContext` type.

use axum::{
    Extension,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::RequestContext;
use zradar_traits::{AdminAuthorizer, QueryAuthorizer};

use crate::errors::{ControlError, Result as ApiResult};

pub use zradar_traits::Capability;

/// Auth mode marker injected as an axum extension by the router.
///
/// Currently only `Standalone` is used. Kept as an enum (not `bool`) so callers
/// can match expressively and the type can be extended without breaking handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// Standard auth mode. Capability checks are opt-in: an empty capabilities
    /// list means all checks pass; a non-empty list enforces explicit grants.
    Standalone,
}

/// Resolved auth context available to HTTP handlers as an Axum extractor.
pub struct AuthContext {
    /// Tenant and project resolved by the authenticator.
    ctx: RequestContext,
    /// Auth mode; always `Standalone` in the OSS build.
    mode: AuthMode,
    /// Capabilities resolved by the authenticator or admin authorizer.
    /// Empty in standalone mode — capability checks are always granted.
    capabilities: Vec<Capability>,
}

impl AuthContext {
    /// Create an auth context directly. Used in tests and by the runtime shim.
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

    /// Returns the auth mode.
    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Enforces a zradar capability.
    ///
    /// When the capabilities list is empty (standalone API-key auth), this method
    /// always succeeds — API-key callers have full access. When a non-empty list
    /// is present (gateway wrapper with trusted headers), the requested capability
    /// must be explicitly included or the request is rejected with `403 Forbidden`.
    pub fn require(&self, capability: Capability) -> ApiResult<()> {
        if self.capabilities.is_empty() {
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

    /// Parses the authenticated workspace ID as a UUID.
    pub fn workspace_uuid(&self) -> ApiResult<Uuid> {
        Ok(self.ctx.workspace_id.into())
    }

    /// Returns the authenticated workspace ID string.
    pub fn workspace_id(&self) -> Uuid {
        self.ctx.workspace_id.into()
    }

    /// Enforces that a path workspace matches the authenticated workspace.
    ///
    /// When capabilities are present (gateway wrapper), the authenticated
    /// workspace must match the path parameter to prevent cross-workspace reads.
    /// In standalone mode (empty capabilities list) this is a no-op.
    pub fn enforce_path_workspace(&self, path_workspace: Uuid) -> ApiResult<()> {
        if self.capabilities.is_empty() {
            return Ok(());
        }
        if self.workspace_uuid()? != path_workspace {
            return Err(ControlError::Forbidden(
                "path workspace_id does not match authenticated workspace".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns the workspace, allowing an optional caller-provided override when
    /// capabilities are not set (standalone API-key mode).
    ///
    /// When capabilities are present, the override is ignored and the
    /// authenticated workspace is returned to prevent cross-workspace access.
    pub fn workspace_or_standalone_override(
        &self,
        requested_workspace: Option<Uuid>,
    ) -> ApiResult<Uuid> {
        let workspace_id = self.workspace_uuid()?;
        if self.capabilities.is_empty() {
            Ok(requested_workspace.unwrap_or(workspace_id))
        } else {
            Ok(workspace_id)
        }
    }

    /// Returns the workspace or rejects a cross-workspace override when capabilities are present.
    pub fn workspace_or_reject_platform_override(
        &self,
        requested_workspace: Option<Uuid>,
    ) -> ApiResult<Uuid> {
        let workspace_id = self.workspace_uuid()?;
        if self.capabilities.is_empty() {
            return Ok(requested_workspace.unwrap_or(workspace_id));
        }
        if let Some(requested) = requested_workspace
            && requested != workspace_id
        {
            return Err(ControlError::Forbidden(
                "workspace_id override not allowed when capabilities are enforced".to_string(),
            ));
        }
        Ok(workspace_id)
    }

    /// Returns workspace filters for audit reads.
    ///
    /// When capabilities are present, returns the authenticated workspace
    /// to prevent cross-workspace audit reads.
    pub fn audit_scope(&self, requested_workspace: Option<Uuid>) -> ApiResult<Option<Uuid>> {
        if !self.capabilities.is_empty() {
            Ok(Some(self.workspace_uuid()?))
        } else {
            Ok(requested_workspace)
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

impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(mode): Extension<AuthMode> = Extension::from_request_parts(parts, state)
            .await
            .map_err(|_| AuthError("Auth mode not configured".to_string()))?;

        let resolution = if let Some(query) = parts.extensions.get::<Arc<dyn QueryAuthorizer>>() {
            query
                .authorize(&parts.headers)
                .await
                .map_err(|_| AuthError("Invalid credentials".to_string()))?
        } else if let Some(admin) = parts.extensions.get::<Arc<dyn AdminAuthorizer>>() {
            admin
                .authorize(&parts.headers)
                .await
                .map_err(|_| AuthError("Invalid credentials".to_string()))?
        } else {
            return Err(AuthError(
                "QueryAuthorizer or AdminAuthorizer not configured".to_string(),
            ));
        };

        // Convert wire capability keys into strongly-typed Capability values.
        // Unknown keys are silently dropped — forward-compatible with new scopes.
        let capabilities = resolution
            .capability_keys
            .iter()
            .filter_map(|key| parse_capability_key(key))
            .collect();

        Ok(AuthContext {
            ctx: resolution.context,
            mode,
            capabilities,
        })
    }
}

/// Convert a wire capability key string into a `Capability` variant.
fn parse_capability_key(key: &str) -> Option<Capability> {
    Capability::from_key(key)
}

/// Parses a UUID string from trusted request context.
pub fn parse_ctx_uuid(value: &str, field: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(value).map_err(|_| {
        ControlError::InvalidInput(format!("invalid {field} in request context: {value}"))
    })
}

/// Parses `x-zradar-capabilities` header values into `Capability` enum values.
///
/// Provided as a convenience for tests and gateway wrappers that build
/// capability lists from a comma-separated header before constructing an
/// `AdminAuth` response.
pub fn parse_capabilities(header: &str) -> Vec<Capability> {
    header
        .split(',')
        .filter_map(|p| Capability::from_key(p.trim()))
        .collect()
}
