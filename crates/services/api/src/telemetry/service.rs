//! Query service - telemetry use case orchestration

use chrono::DateTime;
use std::sync::Arc;
use uuid::Uuid;

use super::types::{
    AnalyticsQuery, AnalyticsResult, ErrorAnalyticsQuery, ErrorBreakdown, LogDetail,
    LogQueryFilters, MetricDetail, MetricQueryFilters, MetricSeriesFilters, MetricSeriesPoint,
    PaginatedResponse, SpanDetail, SpanQueryFilters, TopEndpoint, TopNQuery, TraceDetail,
    TraceQueryFilters, TraceSummary,
};
use crate::errors::{ControlError, Result};

// Use storage-level traits from zradar-traits
use zradar_traits::{
    LogQueryFilters as StorageLogFilters, MetricQueryFilters as StorageMetricFilters,
    MetricSeriesFilters as StorageMetricSeriesFilters, Pagination,
    SpanQueryFilters as StorageSpanFilters, TelemetryReader as StorageTelemetryReader, TimeRange,
    TraceQueryFilters as StorageTraceFilters,
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
            time_range: filters
                .start_time
                .zip(filters.end_time)
                .map(|(start, end)| TimeRange {
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
        let result = self
            .storage
            .query_traces(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        // Convert storage types to API types
        let traces: Vec<TraceSummary> = result
            .items
            .into_iter()
            .map(|t| TraceSummary {
                trace_id: t.trace_id,
                start_time: DateTime::from_timestamp_nanos(t.start_time),
                duration_ms: t.duration_ms,
                span_count: t.span_count,
                service_name: t.service_name,
                operation_name: t.trace_name,
                status: if t.has_error != 0 {
                    "ERROR".to_string()
                } else {
                    "OK".to_string()
                },
            })
            .collect();

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
        let spans = self
            .storage
            .get_trace_detail(project_id, trace_id)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let spans = spans.ok_or_else(|| ControlError::NotFound("Trace not found".to_string()))?;

        if spans.is_empty() {
            return Err(ControlError::NotFound("Trace not found".to_string()));
        }

        // Convert storage spans to API span details
        let span_details: Vec<SpanDetail> = spans
            .iter()
            .map(|s| SpanDetail {
                trace_id: s.trace_id.clone(),
                span_id: s.span_id.clone(),
                parent_span_id: if s.parent_span_id.is_empty() {
                    None
                } else {
                    Some(s.parent_span_id.clone())
                },
                operation_name: s.span_name.clone(),
                service_name: s.service_name.clone(),
                span_type: s.span_type.clone(),
                start_time: DateTime::from_timestamp_nanos(s.timestamp),
                duration_ms: (s.duration_ns / 1_000_000),
                status: s.status_code.clone(),
                attributes: serde_json::from_str(&s.attributes).unwrap_or_default(),
            })
            .collect();

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
        let span_types = filters
            .parse_span_types()
            .map_err(ControlError::InvalidInput)?;

        // Convert API filters to storage filters
        let storage_filters = StorageSpanFilters {
            project_id: Some(project_id),
            trace_id: filters.trace_id.clone(),
            time_range: filters
                .start_time
                .zip(filters.end_time)
                .map(|(start, end)| TimeRange {
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
        let result = self
            .storage
            .query_spans(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        // Convert storage spans to API types
        let spans: Vec<SpanDetail> = result
            .items
            .into_iter()
            .map(|s| SpanDetail {
                trace_id: s.trace_id,
                span_id: s.span_id,
                parent_span_id: if s.parent_span_id.is_empty() {
                    None
                } else {
                    Some(s.parent_span_id)
                },
                operation_name: s.span_name,
                service_name: s.service_name,
                span_type: s.span_type,
                start_time: DateTime::from_timestamp_nanos(s.timestamp),
                duration_ms: s.duration_ns / 1_000_000,
                status: s.status_code,
                attributes: serde_json::from_str(&s.attributes).unwrap_or_default(),
            })
            .collect();

        Ok(PaginatedResponse {
            items: spans,
            total: result.total as i64,
            page: 0,
            page_size: result.limit as i64,
        })
    }

    /// Get a single span by its ID.
    pub async fn get_span(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        project_id: Uuid,
        span_id: &str,
    ) -> Result<SpanDetail> {
        let span = self
            .storage
            .get_span(project_id, span_id)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?
            .ok_or_else(|| ControlError::NotFound("Span not found".to_string()))?;

        Ok(SpanDetail {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: if span.parent_span_id.is_empty() {
                None
            } else {
                Some(span.parent_span_id)
            },
            operation_name: span.span_name,
            service_name: span.service_name,
            span_type: span.span_type,
            start_time: DateTime::from_timestamp_nanos(span.timestamp),
            duration_ms: span.duration_ns / 1_000_000,
            status: span.status_code,
            attributes: serde_json::from_str(&span.attributes).unwrap_or_default(),
        })
    }

    /// Get analytics (daily trace counts)
    pub async fn get_analytics(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        query: AnalyticsQuery,
    ) -> Result<Vec<AnalyticsResult>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let end = query
            .end
            .map(|d| d.timestamp_nanos_opt().unwrap_or(0))
            .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());

        let start = query
            .start
            .map(|d| d.timestamp_nanos_opt().unwrap_or(0))
            .unwrap_or_else(|| end - 7 * 24 * 60 * 60 * 1_000_000_000); // Default 7 days

        let points = self
            .storage
            .get_daily_trace_counts(project_id, start, end)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let results = points
            .into_iter()
            .map(|p| AnalyticsResult {
                timestamp: p.timestamp,
                value: p.value,
                // Add other fields if necessary, or use default
                ..Default::default()
            })
            .collect();

        Ok(results)
    }

    /// Get metrics summary
    pub async fn get_metrics_summary(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        query: AnalyticsQuery,
    ) -> Result<crate::telemetry::types::MetricsSummary> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let end = query
            .end
            .map(|d| d.timestamp_nanos_opt().unwrap_or(0))
            .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());

        let start = query
            .start
            .map(|d| d.timestamp_nanos_opt().unwrap_or(0))
            .unwrap_or_else(|| end - 7 * 24 * 60 * 60 * 1_000_000_000);

        let summary = self
            .storage
            .get_metrics_summary(project_id, start, end)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        // Convert trait MetricsSummary to API MetricsSummary
        Ok(crate::telemetry::types::MetricsSummary {
            total_traces: summary.total_traces,
            error_rate: summary.error_rate,
            p50_latency: summary.p50_latency,
            p90_latency: summary.p90_latency,
            p99_latency: summary.p99_latency,
        })
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

    /// Query log records
    pub async fn query_logs(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        filters: LogQueryFilters,
    ) -> Result<PaginatedResponse<LogDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let storage_filters = StorageLogFilters {
            project_id: Some(project_id),
            time_range: filters
                .start_time
                .zip(filters.end_time)
                .map(|(start, end)| TimeRange {
                    start: start.timestamp_nanos_opt().unwrap_or(0),
                    end: end.timestamp_nanos_opt().unwrap_or(0),
                }),
            severity: filters.severity,
            service_name: filters.service_name,
            trace_id: filters.trace_id,
            search_text: filters.search_text,
            agent_name: filters.agent_name,
            session_id: filters.session_id,
            pagination: Pagination {
                limit: Some(filters.limit.unwrap_or(100) as u32),
                offset: Some(0),
            },
        };

        let result = self
            .storage
            .query_logs(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let logs: Vec<LogDetail> = result
            .items
            .into_iter()
            .map(|l| LogDetail {
                id: l.id,
                timestamp: DateTime::from_timestamp_nanos(l.timestamp),
                severity: l.severity,
                service_name: l.service_name,
                message: l.message,
                trace_id: if l.trace_id.is_empty() {
                    None
                } else {
                    Some(l.trace_id)
                },
                span_id: if l.span_id.is_empty() {
                    None
                } else {
                    Some(l.span_id)
                },
                agent_name: if l.agent_name.is_empty() {
                    None
                } else {
                    Some(l.agent_name)
                },
                session_id: if l.session_id.is_empty() {
                    None
                } else {
                    Some(l.session_id)
                },
                user_id: if l.user_id.is_empty() {
                    None
                } else {
                    Some(l.user_id)
                },
                attributes: serde_json::from_str(&l.attributes).unwrap_or_default(),
            })
            .collect();

        Ok(PaginatedResponse {
            items: logs,
            total: result.total as i64,
            page: 0,
            page_size: result.limit as i64,
        })
    }

    /// Get a single log by ID
    pub async fn get_log(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        project_id: Uuid,
        log_id: &str,
    ) -> Result<LogDetail> {
        let log = self
            .storage
            .get_log(project_id, log_id)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?
            .ok_or_else(|| ControlError::NotFound("Log not found".to_string()))?;

        Ok(LogDetail {
            id: log.id,
            timestamp: DateTime::from_timestamp_nanos(log.timestamp),
            severity: log.severity,
            service_name: log.service_name,
            message: log.message,
            trace_id: if log.trace_id.is_empty() {
                None
            } else {
                Some(log.trace_id)
            },
            span_id: if log.span_id.is_empty() {
                None
            } else {
                Some(log.span_id)
            },
            agent_name: if log.agent_name.is_empty() {
                None
            } else {
                Some(log.agent_name)
            },
            session_id: if log.session_id.is_empty() {
                None
            } else {
                Some(log.session_id)
            },
            user_id: if log.user_id.is_empty() {
                None
            } else {
                Some(log.user_id)
            },
            attributes: serde_json::from_str(&log.attributes).unwrap_or_default(),
        })
    }

    /// Query metrics
    pub async fn query_metrics(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        filters: MetricQueryFilters,
    ) -> Result<PaginatedResponse<MetricDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let storage_filters = StorageMetricFilters {
            project_id: Some(project_id),
            time_range: filters
                .start_time
                .zip(filters.end_time)
                .map(|(start, end)| TimeRange {
                    start: start.timestamp_nanos_opt().unwrap_or(0),
                    end: end.timestamp_nanos_opt().unwrap_or(0),
                }),
            metric_name: filters.metric_name,
            service_name: filters.service_name,
            agent_name: filters.agent_name,
            pagination: Pagination {
                limit: Some(filters.limit.unwrap_or(100) as u32),
                offset: Some(0),
            },
        };

        let result = self
            .storage
            .query_metrics(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let metrics: Vec<MetricDetail> = result
            .items
            .into_iter()
            .map(|m| MetricDetail {
                metric_name: m.metric_name,
                metric_type: m.metric_type,
                timestamp: DateTime::from_timestamp_nanos(m.timestamp),
                service_name: m.service_name,
                value: m.value,
                count: m.count,
                sum: m.sum,
                min: m.min,
                max: m.max,
                labels: serde_json::from_str(&m.labels).unwrap_or_default(),
            })
            .collect();

        Ok(PaginatedResponse {
            items: metrics,
            total: result.total as i64,
            page: 0,
            page_size: result.limit as i64,
        })
    }

    /// Query metric time-series (bucketed aggregates)
    pub async fn query_metric_series(
        &self,
        _user_id: Uuid,
        _org_id: Uuid,
        filters: MetricSeriesFilters,
    ) -> Result<Vec<MetricSeriesPoint>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let storage_filters = StorageMetricSeriesFilters {
            project_id: Some(project_id),
            metric_name: filters.metric_name,
            time_range: filters
                .start_time
                .zip(filters.end_time)
                .map(|(start, end)| TimeRange {
                    start: start.timestamp_nanos_opt().unwrap_or(0),
                    end: end.timestamp_nanos_opt().unwrap_or(0),
                }),
            interval_seconds: filters.interval_seconds.unwrap_or(60),
            aggregation: filters.aggregation.unwrap_or_else(|| "avg".to_string()),
            service_name: filters.service_name,
        };

        let points = self
            .storage
            .query_metric_series(storage_filters)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let series: Vec<MetricSeriesPoint> = points
            .into_iter()
            .map(|p| MetricSeriesPoint {
                timestamp: DateTime::from_timestamp_nanos(p.bucket_ts),
                value: p.value,
            })
            .collect();

        Ok(series)
    }
}
