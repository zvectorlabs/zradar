//! Telemetry repository traits

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zradar_models::{Span, Metric};

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
    pub start: i64,  // Unix timestamp nanos
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
    pub pagination: Pagination,
}

/// Trace summary
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TraceSummary {
    pub trace_id: String,
    pub trace_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub duration_ms: i64,         // i64 for PostgreSQL compat
    pub span_count: i64,          // i64 for PostgreSQL compat (COUNT returns BIGINT)
    pub service_name: String,
    pub has_error: i16,           // i16 for PostgreSQL compat (SMALLINT)
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
/// Used for inserting spans and metrics into storage.
#[async_trait]
pub trait TelemetryWriter: Send + Sync {
    /// Insert spans
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()>;
    
    /// Insert metrics
    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()>;
}

// ============================================================================
// Reader trait
// ============================================================================

/// Telemetry reader trait
/// 
/// Used for querying telemetry data.
#[async_trait]
pub trait TelemetryReader: Send + Sync {
    /// Query traces
    async fn query_traces(&self, filters: TraceQueryFilters) -> anyhow::Result<PaginatedResponse<TraceSummary>>;
    
    /// Get trace detail with all spans
    async fn get_trace_detail(&self, project_id: Uuid, trace_id: &str) -> anyhow::Result<Option<Vec<Span>>>;
    
    /// Query spans
    async fn query_spans(&self, filters: SpanQueryFilters) -> anyhow::Result<PaginatedResponse<Span>>;
    
    /// Get span by ID
    async fn get_span(&self, project_id: Uuid, span_id: &str) -> anyhow::Result<Option<Span>>;
}

