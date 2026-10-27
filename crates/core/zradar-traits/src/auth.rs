//! Authentication trait

use async_trait::async_trait;
use axum::http::HeaderMap;
use zradar_models::RequestContext;

/// Validates an OTLP ingest bearer token and returns the request context.
///
/// The default implementation (`ConfigAuthenticator`) checks the token against
/// a static map loaded from the server configuration file.
///
/// # Errors
///
/// - A rejected credential (unknown, revoked, malformed) is any error; the
///   transports answer `UNAUTHENTICATED` / `401`, which exporters do not retry.
/// - When the credential could not be checked because the verification backend
///   is unreachable, timed out or failed, return
///   [`AuthUnavailable`](crate::AuthUnavailable) (anywhere in the `anyhow`
///   chain). The transports then answer `UNAVAILABLE` / `503` with a retry
///   delay so exporters retry instead of dropping data.
///
/// Error messages may be logged; never include the token in them.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext>;

    /// Authenticate with access to every request header (HTTP headers on
    /// OTLP/HTTP, all request metadata on OTLP/gRPC), for implementations that
    /// resolve context from headers besides `authorization`.
    ///
    /// Called before the request body is read or decoded. The default
    /// implementation ignores `headers` and calls [`Self::authenticate`].
    async fn authenticate_request(
        &self,
        token: &str,
        headers: &HeaderMap,
    ) -> anyhow::Result<RequestContext> {
        let _ = headers;
        self.authenticate(token).await
    }
}
