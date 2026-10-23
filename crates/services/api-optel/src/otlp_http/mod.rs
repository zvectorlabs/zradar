//! OTLP/HTTP receiver on :4318 (R1.1).
//!
//! Accepts protobuf-encoded OTLP exports over HTTP/1.1 and HTTP/2.
//! JSON body is rejected with 415 Unsupported Media Type.
//! Body size is capped (8 MiB by default; see [`otlp_http_router_with_body_limit`]).
//! [`serve`] bounds how long a client may take to send request headers and
//! body (slowloris protection); see [`OtlpHttpServerOptions`].
//!
//! Routes:
//!   POST /v1/traces   → ExportTraceServiceRequest
//!   POST /v1/metrics  → ExportMetricsServiceRequest
//!   POST /v1/logs     → ExportLogsServiceRequest

mod handlers;
mod server;

pub use handlers::{DEFAULT_MAX_BODY_BYTES, otlp_http_router, otlp_http_router_with_body_limit};
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
}
