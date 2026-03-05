//! # zradar-otlp
//!
//! OpenTelemetry Protocol (OTLP) implementation for zradar.
//!
//! This crate provides:
//! - OTLP Trace Service (gRPC)
//! - OTLP Metrics Service (gRPC)
//! - OTLP Logs Service (gRPC)
//! - Converters from OTLP protobuf to internal models

mod auth;
mod converter;
mod direct_handler;
mod logs_converter;
mod logs_service;
mod metrics_converter;
mod metrics_service;
mod span_handler;
mod span_type_mapper;
mod trace_service;

pub use auth::{ApiKeyAuth, DbApiKeyAuth};
pub use converter::OtlpConverter;
pub use direct_handler::{
    DirectLogHandler, DirectLogsHandler, DirectMetricsHandler, DirectSpanHandler, NullLogHandler,
    NullScoreHandler,
};
pub use logs_converter::OtlpLogsConverter;
pub use logs_service::{LogHandler, OtlpLogsService, ScoreHandler};
pub use metrics_converter::OtlpMetricsConverter;
pub use metrics_service::{MetricHandler, OtlpMetricsService};
pub use span_handler::JobQueueSpanHandler;
pub use span_type_mapper::SpanTypeMapper;
pub use trace_service::{OtlpTraceService, SpanHandler};

// Re-export OTLP types for convenience
pub use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, trace_service_server::TraceServiceServer,
};

pub use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, metrics_service_server::MetricsServiceServer,
};

pub use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, logs_service_server::LogsServiceServer,
};
