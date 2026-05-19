//! Telemetry types and DTOs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

// Re-export TelemetryWriter from traits
pub use zradar_traits::TelemetryWriter;

/// Trace query filters
#[derive(Debug, Deserialize, ToSchema)]
pub struct TraceQueryFilters {
    #[serde(default)]
    pub project_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub status: Option<String>,
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<i64>,
}

/// Span query filters
#[derive(Debug, Deserialize, ToSchema)]
pub struct SpanQueryFilters {
    #[serde(default)]
    pub project_id: String,
    pub trace_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub span_type: Option<String>,
    pub span_types: Option<String>, // comma-separated list
    pub llm_model: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<i64>,
}

impl SpanQueryFilters {
    /// Parse and validate span_types
    pub fn parse_span_types(&self) -> Result<Option<Vec<String>>, String> {
        if let Some(ref types_str) = self.span_types {
            let types: Vec<String> = types_str.split(',').map(|s| s.trim().to_string()).collect();

            // Validate each type
            for t in &types {
                if !zradar_models::Span::validate_span_type(t) {
                    return Err(format!("Invalid span_type: {}", t));
                }
            }
            Ok(Some(types))
        } else if let Some(ref single_type) = self.span_type {
            if !zradar_models::Span::validate_span_type(single_type) {
                return Err(format!("Invalid span_type: {}", single_type));
            }
            Ok(Some(vec![single_type.clone()]))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_span_type() {
        let params = SpanQueryFilters {
            project_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: Some("GENERATION".to_string()),
            span_types: None,
            llm_model: None,
            agent_name: None,
            session_id: None,
            limit: None,
        };
        let result = params.parse_span_types().unwrap();
        assert_eq!(result, Some(vec!["GENERATION".to_string()]));
    }

    #[test]
    fn test_parse_multiple_span_types() {
        let params = SpanQueryFilters {
            project_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: None,
            span_types: Some("GENERATION,TOOL,AGENT".to_string()),
            llm_model: None,
            agent_name: None,
            session_id: None,
            limit: None,
        };
        let result = params.parse_span_types().unwrap();
        assert_eq!(
            result,
            Some(vec![
                "GENERATION".to_string(),
                "TOOL".to_string(),
                "AGENT".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_invalid_span_type() {
        let params = SpanQueryFilters {
            project_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: Some("INVALID".to_string()),
            span_types: None,
            llm_model: None,
            agent_name: None,
            session_id: None,
            limit: None,
        };
        assert!(params.parse_span_types().is_err());
    }

    #[test]
    fn test_parse_span_types_with_whitespace() {
        let params = SpanQueryFilters {
            project_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: None,
            span_types: Some(" GENERATION , TOOL ".to_string()),
            llm_model: None,
            agent_name: None,
            session_id: None,
            limit: None,
        };
        let result = params.parse_span_types().unwrap();
        assert_eq!(
            result,
            Some(vec!["GENERATION".to_string(), "TOOL".to_string()])
        );
    }
}

/// Analytics query
#[derive(Debug, Deserialize, ToSchema)]
pub struct AnalyticsQuery {
    #[serde(default)]
    pub project_id: String,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub metric: Option<String>,
    /// Comma-separated list of dimensions to group by, e.g. "agent_name,llm_model"
    pub group_by: Option<String>,
    pub filters: Option<HashMap<String, String>>,
}

/// Top N query
#[derive(Debug, Deserialize, ToSchema)]
pub struct TopNQuery {
    #[serde(default)]
    pub project_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub n: Option<i64>,
}

/// Trace summary
#[derive(Debug, Serialize, ToSchema)]
pub struct TraceSummary {
    pub trace_id: String,
    pub start_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub service_name: String,
    pub operation_name: String,
    pub status: String,
    pub span_count: i64,
}

/// Trace detail
#[derive(Debug, Serialize, ToSchema)]
pub struct TraceDetail {
    pub trace_id: String,
    pub start_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub spans: Vec<SpanDetail>,
}

/// Span detail
#[derive(Debug, Serialize, ToSchema)]
pub struct SpanDetail {
    pub span_id: String,
    pub trace_id: String,
    pub parent_span_id: Option<String>,
    pub service_name: String,
    pub operation_name: String,
    pub span_type: String, // SPAN, EVENT, GENERATION, AGENT, TOOL, etc.
    pub start_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub status: String,
    pub attributes: HashMap<String, String>,
}

/// Analytics result
#[derive(Debug, Serialize, ToSchema, Default)]
pub struct AnalyticsResult {
    pub timestamp: String,
    pub value: f64,
    pub groups: Option<HashMap<String, String>>,
}

/// Metrics summary
#[derive(Debug, Serialize, ToSchema)]
pub struct MetricsSummary {
    pub total_traces: i64,
    pub error_rate: f64,
    pub p50_latency: f64,
    pub p90_latency: f64,
    pub p99_latency: f64,
}

/// Top endpoint
#[derive(Debug, Serialize, ToSchema)]
pub struct TopEndpoint {
    pub operation_name: String,
    pub service_name: String,
    pub count: i64,
    pub avg_duration_ms: f64,
    pub p95_duration_ms: Option<f64>,
    pub error_rate: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LlmAnalytics {
    pub llm_model: String,
    pub request_count: i64,
    pub total_tokens: f64,
    pub total_cost_usd: f64,
    pub avg_duration_ms: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AgentAnalytics {
    pub agent_name: String,
    pub agent_type: Option<String>,
    pub span_count: i64,
    pub error_count: i64,
    pub total_tokens: f64,
    pub avg_duration_ms: f64,
}

/// Paginated response
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

/// Error analytics query
#[derive(Debug, Deserialize, ToSchema)]
pub struct ErrorAnalyticsQuery {
    #[serde(default)]
    pub project_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub service_name: Option<String>,
}

/// Error breakdown
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorBreakdown {
    pub error_type: String,
    pub count: i64,
    pub percentage: f64,
}

/// Log query filters (API-level)
#[derive(Debug, Deserialize, ToSchema)]
pub struct LogQueryFilters {
    #[serde(default)]
    pub project_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub severity: Option<String>,
    pub service_name: Option<String>,
    pub trace_id: Option<String>,
    pub search_text: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<i64>,
}

/// Metric query filters (API-level)
#[derive(Debug, Deserialize, ToSchema)]
pub struct MetricQueryFilters {
    #[serde(default)]
    pub project_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub metric_name: Option<String>,
    pub service_name: Option<String>,
    pub agent_name: Option<String>,
    pub limit: Option<i64>,
}

/// Metric time-series filters (API-level)
#[derive(Debug, Deserialize, ToSchema)]
pub struct MetricSeriesFilters {
    #[serde(default)]
    pub project_id: String,
    pub metric_name: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub interval_seconds: Option<u64>,
    pub aggregation: Option<String>,
    pub service_name: Option<String>,
}

/// Log record detail (API-level response)
#[derive(Debug, Serialize, ToSchema)]
pub struct LogDetail {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub severity: String,
    pub service_name: String,
    pub message: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Metric data point (API-level response)
#[derive(Debug, Serialize, ToSchema)]
pub struct MetricDetail {
    pub metric_name: String,
    pub metric_type: String,
    pub timestamp: DateTime<Utc>,
    pub service_name: String,
    pub value: f64,
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub labels: HashMap<String, serde_json::Value>,
}

/// Metric time-series point (API-level response)
#[derive(Debug, Serialize, ToSchema)]
pub struct MetricSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

// Note: We no longer define a TelemetryReader trait at the API level.
// The service layer (QueryService) handles conversion between API DTOs and storage calls.
// Storage implementations use zradar_traits::TelemetryReader.
