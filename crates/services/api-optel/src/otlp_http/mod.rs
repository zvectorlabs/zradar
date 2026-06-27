//! OTLP/HTTP receiver on :4318 (R1.1).
//!
//! Accepts protobuf-encoded OTLP exports over HTTP/1.1 and HTTP/2.
//! JSON body is rejected with 415 Unsupported Media Type.
//! Body size is capped at 8 MiB.
//!
//! Routes:
//!   POST /v1/traces   → ExportTraceServiceRequest
//!   POST /v1/metrics  → ExportMetricsServiceRequest
//!   POST /v1/logs     → ExportLogsServiceRequest

mod handlers;

pub use handlers::otlp_http_router;

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
