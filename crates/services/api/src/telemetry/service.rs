//! Query service - telemetry use case orchestration

use std::sync::Arc;
use uuid::Uuid;
use chrono::DateTime;

use super::types::{
    TraceSummary, TraceDetail, SpanDetail, PaginatedResponse,
    TraceQueryFilters, SpanQueryFilters, AnalyticsQuery, AnalyticsResult,
    TopNQuery, TopEndpoint, ErrorAnalyticsQuery, ErrorBreakdown,
};
use crate::errors::{ControlError, Result};

// Use storage-level traits from zradar-traits
use zradar_traits::{
    TelemetryReader as StorageTelemetryReader,
    TraceQueryFilters as StorageTraceFilters,
    SpanQueryFilters as StorageSpanFilters,
    Pagination, TimeRange,
};

/// Query service for telemetry operations
pub struct QueryService {
    pub storage: Arc<dyn StorageTelemetryReader>,
}

impl QueryService {
    /// Create a new QueryService
    pub fn new(storage: Arc<dyn StorageTelemetryReader>) -> Self {
        Self { storage }
    }

    /// Query traces
    pub async fn query_traces(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        filters: TraceQueryFilters,
    ) -> Result<PaginatedResponse<TraceSummary>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        
        // Convert API filters to storage filters
        let storage_filters = StorageTraceFilters {
            project_id: Some(project_id),
            time_range: filters.start_time.zip(filters.end_time).map(|(start, end)| TimeRange {
                start: start.timestamp_nanos_opt().unwrap_or(0),
                end: end.timestamp_nanos_opt().unwrap_or(0),
            }),
            service_name: filters.service_name.clone(),
            status: filters.status.clone(),
            min_duration_ms: filters.min_duration_ms.map(|d| d as u64),
            max_duration_ms: filters.max_duration_ms.map(|d| d as u64),
            pagination: Pagination {
                limit: Some(filters.limit.unwrap_or(100) as u32),
                offset: Some(0),
            },
        };
        
        // Query storage
        let result = self.storage.query_traces(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;
        
        // Convert storage types to API types
        let traces: Vec<TraceSummary> = result.items.into_iter().map(|t| TraceSummary {
            trace_id: t.trace_id,
            start_time: DateTime::from_timestamp_nanos(t.start_time),
            duration_ms: t.duration_ms,
            span_count: t.span_count,
            service_name: t.service_name,
            operation_name: t.trace_name,
            status: if t.has_error != 0 { "ERROR".to_string() } else { "OK".to_string() },
        }).collect();
        
        Ok(PaginatedResponse {
            items: traces,
            total: result.total as i64,
            page: 0,
            page_size: result.limit as i64,
        })
    }

    /// Get trace detail
    pub async fn get_trace(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<TraceDetail> {
        // Query storage for spans in this trace
        let spans = self.storage.get_trace_detail(project_id, trace_id)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;
        
        let spans = spans.ok_or_else(|| ControlError::NotFound("Trace not found".to_string()))?;
        
        if spans.is_empty() {
            return Err(ControlError::NotFound("Trace not found".to_string()));
        }
        
        // Convert storage spans to API span details
        let span_details: Vec<SpanDetail> = spans.iter().map(|s| SpanDetail {
            trace_id: s.trace_id.clone(),
            span_id: s.span_id.clone(),
            parent_span_id: if s.parent_span_id.is_empty() { None } else { Some(s.parent_span_id.clone()) },
            operation_name: s.span_name.clone(),
            service_name: s.service_name.clone(),
            span_type: s.span_type.clone(),
            start_time: DateTime::from_timestamp_nanos(s.timestamp),
            duration_ms: (s.duration_ns / 1_000_000),
            status: s.status_code.clone(),
            attributes: serde_json::from_str(&s.attributes).unwrap_or_default(),
        }).collect();
        
        // Build trace summary from spans
        let root = &spans[0];
        let total_duration_ns: i64 = spans.iter().map(|s| s.duration_ns).sum();
        
        Ok(TraceDetail {
            trace_id: trace_id.to_string(),
            start_time: DateTime::from_timestamp_nanos(root.timestamp),
            duration_ms: total_duration_ns / 1_000_000,
            spans: span_details,
        })
    }

    /// Query spans
    pub async fn query_spans(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        filters: SpanQueryFilters,
    ) -> Result<PaginatedResponse<SpanDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        
        // Parse and validate span_types
        let span_types = filters.parse_span_types()
            .map_err(|e| ControlError::InvalidInput(e))?;
        
        // Convert API filters to storage filters
        let storage_filters = StorageSpanFilters {
            project_id: Some(project_id),
            trace_id: filters.trace_id.clone(),
            time_range: filters.start_time.zip(filters.end_time).map(|(start, end)| TimeRange {
                start: start.timestamp_nanos_opt().unwrap_or(0),
                end: end.timestamp_nanos_opt().unwrap_or(0),
            }),
            service_name: filters.service_name.clone(),
            span_name: filters.operation_name.clone(),
            span_types: span_types.clone(),
            status: None,
            pagination: Pagination {
                limit: Some(filters.limit.unwrap_or(100) as u32),
                offset: Some(0),
            },
        };
        
        // Query storage
        let result = self.storage.query_spans(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;
        
        // Convert storage spans to API types
        let spans: Vec<SpanDetail> = result.items.into_iter().map(|s| SpanDetail {
            trace_id: s.trace_id,
            span_id: s.span_id,
            parent_span_id: if s.parent_span_id.is_empty() { None } else { Some(s.parent_span_id) },
            operation_name: s.span_name,
            service_name: s.service_name,
            span_type: s.span_type,
            start_time: DateTime::from_timestamp_nanos(s.timestamp),
            duration_ms: s.duration_ns / 1_000_000,
            status: s.status_code,
            attributes: serde_json::from_str(&s.attributes).unwrap_or_default(),
        }).collect();
        
        Ok(PaginatedResponse {
            items: spans,
            total: result.total as i64,
            page: 0,
            page_size: result.limit as i64,
        })
    }

    /// Get analytics
    pub async fn get_analytics(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        _query: AnalyticsQuery,
    ) -> Result<Vec<AnalyticsResult>> {
        // TODO: Implement analytics
        Ok(vec![])
    }

    /// Get top endpoints
    pub async fn get_top_endpoints(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        _query: TopNQuery,
    ) -> Result<Vec<TopEndpoint>> {
        // TODO: Implement top endpoints
        Ok(vec![])
    }

    /// Get error breakdown
    pub async fn get_error_breakdown(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        _query: ErrorAnalyticsQuery,
    ) -> Result<Vec<ErrorBreakdown>> {
        // TODO: Implement error breakdown
        Ok(vec![])
    }
}
