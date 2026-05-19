//! # api-optel
//!
//! OpenTelemetry Protocol (OTLP) gRPC services for zradar.
//!
//! Provides trace, metrics, and logs ingestion over gRPC.
//! Authentication is handled via `Arc<dyn Authenticator>` from `zradar-traits`.

mod auth;
mod circuit_breaker;
mod converter;
mod direct_handler;
mod ingestion_guard;
mod logs_converter;
mod logs_service;
mod metrics_converter;
mod metrics_service;
mod rate_limiter;
mod span_type_mapper;
mod trace_service;

pub use circuit_breaker::CircuitBreaker;
pub use converter::OtlpConverter;
pub use logs_converter::OtlpLogsConverter;
pub use logs_service::OtlpLogsService;
pub use metrics_converter::OtlpMetricsConverter;
pub use metrics_service::OtlpMetricsService;
pub use rate_limiter::ProjectRateLimiter;
pub use span_type_mapper::SpanTypeMapper;
pub use trace_service::OtlpTraceService;

// Re-export OTLP server types
pub use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, logs_service_server::LogsServiceServer,
};
pub use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, metrics_service_server::MetricsServiceServer,
};
pub use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, trace_service_server::TraceServiceServer,
};
