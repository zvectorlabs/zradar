//! Telemetry repository traits

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zradar_models::{LogRecord, Metric, Span};

// ============================================================================
// Query types
// ============================================================================

/// Pagination info
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Pagination {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Time range filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: i64, // Unix timestamp nanos
    pub end: i64,
}

/// Trace query filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceQueryFilters {
    pub project_id: Option<Uuid>,
    pub time_range: Option<TimeRange>,
    pub service_name: Option<String>,
    pub status: Option<String>,
    pub min_duration_ms: Option<u64>,
    pub max_duration_ms: Option<u64>,
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub pagination: Pagination,
}

/// Span query filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpanQueryFilters {
    pub project_id: Option<Uuid>,
    pub trace_id: Option<String>,
    pub time_range: Option<TimeRange>,
    pub service_name: Option<String>,
    pub span_name: Option<String>,
    pub span_types: Option<Vec<String>>, // Filter by span_type(s)
    pub status: Option<String>,
    pub llm_model: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub pagination: Pagination,
}

/// Log query filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LogQueryFilters {
    pub project_id: Option<Uuid>,
    pub time_range: Option<TimeRange>,
    pub severity: Option<String>,
    pub service_name: Option<String>,
    pub trace_id: Option<String>,
    pub search_text: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub pagination: Pagination,
}

/// Metric query filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricQueryFilters {
    pub project_id: Option<Uuid>,
    pub time_range: Option<TimeRange>,
    pub metric_name: Option<String>,
    pub service_name: Option<String>,
    pub agent_name: Option<String>,
    pub pagination: Pagination,
}

/// Metric time-series (bucketed) filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricSeriesFilters {
    pub project_id: Option<Uuid>,
    pub metric_name: String,
    pub time_range: Option<TimeRange>,
    /// Bucket interval in seconds (e.g. 60 for 1-minute buckets).
    pub interval_seconds: u64,
    /// Aggregation function: "avg", "sum", "min", "max", "count".
    pub aggregation: String,
    pub service_name: Option<String>,
}

/// A single point in a metric time-series response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Bucket start time (nanoseconds).
    pub bucket_ts: i64,
    pub value: f64,
}

/// Trace summary
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TraceSummary {
    pub trace_id: String,
    pub trace_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub duration_ms: i64, // i64 for PostgreSQL compat
    pub span_count: i64,  // i64 for PostgreSQL compat (COUNT returns BIGINT)
    pub service_name: String,
    pub has_error: i16, // i16 for PostgreSQL compat (SMALLINT)
}

/// Paginated response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

// ============================================================================
// Writer trait
// ============================================================================

/// Telemetry writer trait
///
/// Used for inserting spans, metrics, and logs into storage.
#[async_trait]
pub trait TelemetryWriter: Send + Sync {
    /// Insert spans
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()>;

    /// Insert metrics
    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()>;

    /// Insert log records
    async fn insert_logs(&self, logs: &[LogRecord]) -> anyhow::Result<()>;
}

// ============================================================================
// Reader trait
// ============================================================================

/// Telemetry reader trait
///
/// Used for querying telemetry data (traces, spans, logs, metrics).
#[async_trait]
pub trait TelemetryReader: AnalyticsReader + Send + Sync {
    /// Query traces
    async fn query_traces(
        &self,
        filters: TraceQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<TraceSummary>>;

    /// Get trace detail with all spans
    async fn get_trace_detail(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<Vec<Span>>>;

    /// Query spans
    async fn query_spans(
        &self,
        filters: SpanQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<Span>>;

    /// Get span by ID
    async fn get_span(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        span_id: &str,
    ) -> anyhow::Result<Option<Span>>;

    /// Query log records
    async fn query_logs(
        &self,
        filters: LogQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<LogRecord>>;

    /// Get a single log record by its ID
    async fn get_log(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        log_id: &str,
    ) -> anyhow::Result<Option<LogRecord>>;

    /// Query metrics
    async fn query_metrics(
        &self,
        filters: MetricQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<Metric>>;

    /// Query metric time-series (bucketed aggregates)
    async fn query_metric_series(
        &self,
        filters: MetricSeriesFilters,
    ) -> anyhow::Result<Vec<MetricPoint>>;
}

// ============================================================================
// Analytics trait
// ============================================================================

/// Time series data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: String, // ISO8601
    pub value: f64,
}

/// Metrics summary
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsSummary {
    pub total_traces: i64,
    pub error_rate: f64,
    pub p50_latency: f64,
    pub p90_latency: f64,
    pub p99_latency: f64,
}

/// Generic analytics query filters for the storage layer.
#[derive(Debug, Clone, Default)]
pub struct AnalyticsQueryFilters {
    pub project_id: Uuid,
    pub start: i64,
    pub end: i64,
    /// Metric to compute: "trace_count", "span_count", "total_tokens", etc.
    pub metric: String,
    /// Columns to GROUP BY: ["agent_name"], ["llm_model"], ["agent_name", "llm_model"], etc.
    pub group_by: Vec<String>,
    /// WHERE filters on allowed columns: {"agent_name": "planner"}
    pub filters: HashMap<String, String>,
}

/// A single row from a grouped time-series analytics query.
#[derive(Debug, Clone)]
pub struct AnalyticsDataPoint {
    /// Bucket start time (nanoseconds).
    pub bucket_ts: i64,
    /// Aggregated metric value for this bucket + group.
    pub value: f64,
    /// Group dimension values, e.g. {"agent_name": "planner"}.
    pub groups: HashMap<String, String>,
}

/// Analytics reader trait
#[async_trait]
pub trait AnalyticsReader: Send + Sync {
    /// Get daily trace counts
    async fn get_daily_trace_counts(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<TimeSeriesPoint>>;

    /// Get metrics summary
    async fn get_metrics_summary(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<MetricsSummary>;

    /// Generic grouped time-series analytics.
    ///
    /// Dynamically builds a SQL query based on the requested metric, group-by
    /// dimensions, and filters. Returns bucketed time-series data.
    async fn query_analytics(
        &self,
        tenant_id: Uuid,
        filters: AnalyticsQueryFilters,
    ) -> anyhow::Result<Vec<AnalyticsDataPoint>>;
}
