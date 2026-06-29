//! Transport-agnostic authentication/authorization context.
//!
//! `AuthContext` is the service-layer identity envelope: it carries the resolved
//! `RequestContext` (workspace identity) together with the set of capabilities
//! the caller has been granted.  Service trait methods receive `&AuthContext`
//! instead of raw tokens or headers.

use zradar_models::{RequestContext, WorkspaceId};

use crate::capability::Capability;
use crate::errors::ServiceError;

/// Authenticated caller context for the service layer.
///
/// Built by the transport layer (HTTP auth extractor, gRPC interceptor) and
/// passed into every service-trait method that requires authorization.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Resolved workspace / tenant identity.
    pub request_context: RequestContext,
    /// Capabilities granted to this caller.
    /// Empty means "all checks pass" (standalone API-key mode).
    pub capabilities: Vec<Capability>,
}

impl AuthContext {
    /// Create a new `AuthContext`.
    pub fn new(request_context: RequestContext, capabilities: Vec<Capability>) -> Self {
        Self {
            request_context,
            capabilities,
        }
    }

    /// Convenience accessor for the workspace ID.
    pub fn workspace_id(&self) -> WorkspaceId {
        self.request_context.workspace_id
    }

    /// Assert that the caller holds `cap`.
    ///
    /// In standalone mode (empty capabilities list) every check passes.
    /// In gateway mode the capability must be present in the list.
    pub fn require(&self, cap: Capability) -> Result<(), ServiceError> {
        if self.capabilities.is_empty() || self.capabilities.contains(&cap) {
            Ok(())
        } else {
            Err(ServiceError::forbidden(format!(
                "missing capability: {cap:?}"
            )))
        }
    }
}
