//! Authentication trait

use async_trait::async_trait;
use zradar_models::RequestContext;

/// Validates a bearer token and returns the request context.
///
/// The default implementation (`ConfigAuthenticator`) checks the token against
/// a static map loaded from the server configuration file.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext>;
}
