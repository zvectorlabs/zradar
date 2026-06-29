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
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<QueryAuth>;
}
