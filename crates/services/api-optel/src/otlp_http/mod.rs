//! OTLP/HTTP receiver on :4318 (R1.1).
//!
//! Accepts protobuf-encoded OTLP exports over HTTP/1.1 and HTTP/2.
//! JSON body is rejected with 415 Unsupported Media Type.
//! Request bodies may be `gzip` or `zstd` compressed (`Content-Encoding`).
//! Body size is capped on the wire and after decompression (8 MiB by
//! default; see [`OtlpHttpLimits`]). The API key, `Content-Type` and
//! `Content-Encoding` are checked before the body is read.
//! [`serve`] bounds how long a client may take to send request headers and
//! body (slowloris protection); see [`OtlpHttpServerOptions`].
//!
//! Routes:
//!   POST /v1/traces   → ExportTraceServiceRequest
//!   POST /v1/metrics  → ExportMetricsServiceRequest
//!   POST /v1/logs     → ExportLogsServiceRequest

mod encoding;
mod handlers;
mod server;

pub use handlers::{
    DEFAULT_MAX_BODY_BYTES, OtlpHttpLimits, otlp_http_router, otlp_http_router_with_body_limit,
    otlp_http_router_with_limits,
};
pub use server::{OtlpHttpServerOptions, serve};

/// Shared state threaded through all OTLP/HTTP handlers.
#[derive(Clone)]
pub struct OtlpHttpState {
    pub writer: std::sync::Arc<dyn zradar_traits::TelemetryWriter>,
    pub auth: Option<std::sync::Arc<dyn zradar_traits::Authenticator>>,
    pub allow_test_header_context: bool,
    pub settings_repo: Option<std::sync::Arc<dyn zradar_traits::SettingsRepository>>,
    pub rate_limiter: Option<std::sync::Arc<crate::ProjectRateLimiter>>,
    pub policy_enforcer: Option<std::sync::Arc<dyn zradar_policy::PolicyEnforcer>>,
    pub circuit_breaker: Option<std::sync::Arc<crate::CircuitBreaker>>,
    /// Maximum request body size after `Content-Encoding` is removed.
    pub max_decoded_body_bytes: usize,
    /// Maximum time for one API-key verification (`None` = unlimited).
    pub auth_timeout: Option<std::time::Duration>,
}
