//! Admin-path authorizer trait (mutations, policy, audit, settings).

use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::auth_resolution::AuthResolution;

/// Type alias for admin-path authorization results.
pub type AdminAuth = AuthResolution;

/// Validates Admin API requests and resolves workspace/capability context.
///
/// Implementations validate credentials and extract context from request headers.
/// The runtime's HTTP/gRPC layer calls this once per request before routing to handlers.
///
/// OSS builds implement this against the static API-key map.
/// Platform builds implement this against a gateway service token and trusted headers.
#[async_trait]
pub trait AdminAuthorizer: Send + Sync {
    /// Validate the request headers and return the resolved [`AdminAuth`].
    ///
    /// Returns `Err` to reject the request (caller maps to 401 or 403).
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AdminAuth>;
}
