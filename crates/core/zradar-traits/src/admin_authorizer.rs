//! Admin HTTP authorizer trait.
//!
//! The [`AdminAuthorizer`] trait decouples the HTTP admin auth strategy from
//! the server runtime. OSS builds use a config-key–based implementation;
//! platform wrapper builds supply a gateway-backed authorizer.

use async_trait::async_trait;
use axum::http::HeaderMap;

use zradar_models::RequestContext;

/// The resolved result of Admin HTTP authorization.
///
/// Returned by [`AdminAuthorizer::authorize`] and consumed by the `api` crate's
/// auth extractor to build the handler-facing `AuthContext`.
pub struct AdminAuth {
    /// Resolved workspace context.
    pub context: RequestContext,
    /// Zero or more zradar capability identifiers resolved by the authorizer.
    /// In standalone mode this is always empty; handlers pass without capability checks.
    /// In gateway mode this contains the platform-resolved capabilities.
    pub capability_keys: Vec<String>,
}

/// Validates an Admin HTTP request and resolves workspace/capability context.
///
/// Implementations validate credentials and extract context from request headers.
/// The runtime's HTTP layer calls this once per request before routing to handlers.
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
