//! Telemetry and analytics query service traits.
//!
//! These traits abstract the read-path business logic so that both the HTTP
//! (axum) and gRPC (tonic) transports can share the same service implementation.

use async_trait::async_trait;
use zradar_models::{LogRecord, Metric, Span, WorkspaceId};

use crate::errors::ServiceError;
use crate::repositories::telemetry::{
    AnalyticsDataPoint, AnalyticsQueryFilters, GuardrailsAnalyticsFilters,
    GuardrailsAnalyticsResult, LogQueryFilters, MetricPoint, MetricQueryFilters,
    MetricSeriesFilters, MetricsSummary, PaginatedResponse, SpanQueryFilters, TimeSeriesPoint,
    TraceQueryFilters, TraceSummary,
};

/// Telemetry read-path service trait.
///
/// Covers traces, spans, logs, and metrics queries.  The workspace_id is
/// already embedded in each filter struct, but is also taken explicitly so that
/// service implementations can enforce retention / policy before delegating to
/// the storage layer.
#[async_trait]
pub trait TelemetryQueryService: Send + Sync {
    /// List traces matching the given filters.
    async fn query_traces(
        &self,
        workspace_id: WorkspaceId,
        filters: TraceQueryFilters,
    ) -> Result<PaginatedResponse<TraceSummary>, ServiceError>;

    /// Get the full span tree for a single trace.
    async fn get_trace_detail(
        &self,
        workspace_id: WorkspaceId,
        trace_id: &str,
    ) -> Result<Option<Vec<Span>>, ServiceError>;

    /// List spans matching the given filters.
    async fn query_spans(
        &self,
        workspace_id: WorkspaceId,
        filters: SpanQueryFilters,
    ) -> Result<PaginatedResponse<Span>, ServiceError>;

    /// Get a single span by ID.
    async fn get_span(
        &self,
        workspace_id: WorkspaceId,
        span_id: &str,
    ) -> Result<Option<Span>, ServiceError>;

    /// List log records matching the given filters.
    async fn query_logs(
        &self,
        workspace_id: WorkspaceId,
        filters: LogQueryFilters,
    ) -> Result<PaginatedResponse<LogRecord>, ServiceError>;

    /// Get a single log record by ID.
    async fn get_log(
        &self,
        workspace_id: WorkspaceId,
        log_id: &str,
    ) -> Result<Option<LogRecord>, ServiceError>;

    /// List metrics matching the given filters.
    async fn query_metrics(
        &self,
        workspace_id: WorkspaceId,
        filters: MetricQueryFilters,
    ) -> Result<PaginatedResponse<Metric>, ServiceError>;

    /// Query metric time-series (bucketed aggregates).
    async fn query_metric_series(
        &self,
        workspace_id: WorkspaceId,
        filters: MetricSeriesFilters,
    ) -> Result<Vec<MetricPoint>, ServiceError>;
}

/// Analytics query service trait.
///
/// Covers dashboard-style aggregations: daily trace counts, metrics summaries,
/// generic grouped time-series analytics, and guardrails analytics.
#[async_trait]
pub trait AnalyticsQueryService: Send + Sync {
    /// Get daily trace counts for a time range.
    async fn get_daily_trace_counts(
        &self,
        workspace_id: WorkspaceId,
        start: i64,
        end: i64,
    ) -> Result<Vec<TimeSeriesPoint>, ServiceError>;

    /// Get aggregated metrics summary (total traces, error rate, latency percentiles).
    async fn get_metrics_summary(
        &self,
        workspace_id: WorkspaceId,
        start: i64,
        end: i64,
    ) -> Result<MetricsSummary, ServiceError>;

    /// Generic grouped time-series analytics query.
    async fn query_analytics(
        &self,
        workspace_id: WorkspaceId,
        filters: AnalyticsQueryFilters,
    ) -> Result<Vec<AnalyticsDataPoint>, ServiceError>;

    /// Guardrails analytics with halt-rate, rail-type breakdown, and top-halting rails.
    async fn get_guardrails_analytics(
        &self,
        filters: GuardrailsAnalyticsFilters,
    ) -> Result<GuardrailsAnalyticsResult, ServiceError>;
}
