//! Query-path authorizer trait (read-only telemetry and analytics APIs).

use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::auth_resolution::AuthResolution;

/// Type alias for query-path authorization results.
pub type QueryAuth = AuthResolution;

/// Validates Query API requests and resolves workspace/capability context.
///
/// OSS builds implement this against static `[[api_keys]]`.
/// Platform builds implement this against gateway service tokens and trusted headers.
#[async_trait]
pub trait QueryAuthorizer: Send + Sync {
    /// Validate request headers and return resolved auth.
    ///
    /// `headers` holds every request header: the HTTP headers on HTTP routes,
    /// and all request metadata on gRPC (ASCII and `-bin` entries, in their
    /// wire form).
    ///
    /// Return [`AuthUnavailable`](crate::AuthUnavailable) (anywhere in the
    /// `anyhow` chain) when the verification backend is unreachable, timed out
    /// or failed; callers answer a retryable `503` / `UNAVAILABLE` instead of
    /// `401` / `UNAUTHENTICATED`.
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<QueryAuth>;
}
