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
    pub workspace_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub status: Option<String>,
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_response_model: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    // NeMo Phase 2 filters
    pub rail_type: Option<String>,
    pub action_name: Option<String>,
    pub workflow_run_id: Option<String>,
    pub framework: Option<String>,
    pub tool_name: Option<String>,
    pub invocation_id: Option<String>,
    // Phase 4 R4.5 — deployment.environment filter
    pub environment: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Span query filters
#[derive(Debug, Deserialize, ToSchema)]
pub struct SpanQueryFilters {
    #[serde(default)]
    pub workspace_id: String,
    pub trace_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub span_type: Option<String>,
    pub span_types: Option<String>, // comma-separated list
    pub status: Option<String>,
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_response_model: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    // NeMo Phase 2 filters
    pub rail_type: Option<String>,
    pub action_name: Option<String>,
    pub workflow_run_id: Option<String>,
    pub framework: Option<String>,
    pub tool_name: Option<String>,
    pub invocation_id: Option<String>,
    // Phase 4 R4.5 — deployment.environment filter
    pub environment: Option<String>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub db_system_name: Option<String>,
    pub db_operation_name: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
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
            workspace_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: Some("GENERATION".to_string()),
            span_types: None,
            status: None,
            llm_model: None,
            llm_provider: None,
            llm_response_model: None,
            agent_name: None,
            session_id: None,
            rail_type: None,
            action_name: None,
            workflow_run_id: None,
            framework: None,
            tool_name: None,
            invocation_id: None,
            environment: None,
            min_duration_ms: None,
            max_duration_ms: None,
            db_system_name: None,
            db_operation_name: None,
            limit: None,
            offset: None,
        };
        let result = params.parse_span_types().unwrap();
        assert_eq!(result, Some(vec!["GENERATION".to_string()]));
    }

    #[test]
    fn test_parse_multiple_span_types() {
        let params = SpanQueryFilters {
            workspace_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: None,
            span_types: Some("GENERATION,TOOL,AGENT".to_string()),
            status: None,
            llm_model: None,
            llm_provider: None,
            llm_response_model: None,
            agent_name: None,
            session_id: None,
            rail_type: None,
            action_name: None,
            workflow_run_id: None,
            framework: None,
            tool_name: None,
            invocation_id: None,
            environment: None,
            min_duration_ms: None,
            max_duration_ms: None,
            db_system_name: None,
            db_operation_name: None,
            limit: None,
            offset: None,
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
            workspace_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: Some("INVALID".to_string()),
            span_types: None,
            status: None,
            llm_model: None,
            llm_provider: None,
            llm_response_model: None,
            agent_name: None,
            session_id: None,
            rail_type: None,
            action_name: None,
            workflow_run_id: None,
            framework: None,
            tool_name: None,
            invocation_id: None,
            environment: None,
            min_duration_ms: None,
            max_duration_ms: None,
            db_system_name: None,
            db_operation_name: None,
            limit: None,
            offset: None,
        };
        assert!(params.parse_span_types().is_err());
    }

    #[test]
    fn test_parse_span_types_with_whitespace() {
        let params = SpanQueryFilters {
            workspace_id: "test".to_string(),
            trace_id: None,
            start_time: None,
            end_time: None,
            service_name: None,
            operation_name: None,
            span_type: None,
            span_types: Some(" GENERATION , TOOL ".to_string()),
            status: None,
            llm_model: None,
            llm_provider: None,
            llm_response_model: None,
            agent_name: None,
            session_id: None,
            rail_type: None,
            action_name: None,
            workflow_run_id: None,
            framework: None,
            tool_name: None,
            invocation_id: None,
            environment: None,
            min_duration_ms: None,
            max_duration_ms: None,
            db_system_name: None,
            db_operation_name: None,
            limit: None,
            offset: None,
        };
        let result = params.parse_span_types().unwrap();
        assert_eq!(
            result,
            Some(vec!["GENERATION".to_string(), "TOOL".to_string()])
        );
    }

    /// Regression test for R0.1 (NeMo compatibility, Phase 0).
    ///
    /// `SpanDetail.attributes` was previously typed `HashMap<String, String>`,
    /// which made the trace-detail projection silently drop the entire map
    /// whenever any attribute value was non-string (bool, int, array, nested
    /// object) — e.g. Guardrails spans with `{"rail.stop": true}`. Widening
    /// to `HashMap<String, serde_json::Value>` lets arbitrary JSON round-trip
    /// intact through the same `serde_json::from_str(...).unwrap_or_default()`
    /// projection in `service.rs`.
    #[test]
    fn test_spandetail_attributes_preserves_non_string_values() {
        let raw = r#"{"rail.stop": true, "rail.name": "input", "count": 7, "tags": ["a","b"]}"#;
        let attrs: HashMap<String, serde_json::Value> =
            serde_json::from_str(raw).expect("mixed-type JSON must deserialize into Value map");

        assert_eq!(attrs.len(), 4, "all four keys must round-trip");
        assert_eq!(attrs.get("rail.stop"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(
            attrs.get("rail.name"),
            Some(&serde_json::Value::String("input".to_string()))
        );
        assert_eq!(
            attrs.get("count"),
            Some(&serde_json::Value::Number(serde_json::Number::from(7)))
        );
        assert_eq!(
            attrs.get("tags"),
            Some(&serde_json::Value::Array(vec![
                serde_json::Value::String("a".to_string()),
                serde_json::Value::String("b".to_string()),
            ]))
        );

        // Verify the projection-site pattern (unwrap_or_default) also works.
        let projected: HashMap<String, serde_json::Value> =
            serde_json::from_str(raw).unwrap_or_default();
        assert_eq!(
            projected.len(),
            4,
            "unwrap_or_default path must not lose keys"
        );
    }
}

/// Analytics query
#[derive(Debug, Deserialize, ToSchema)]
pub struct AnalyticsQuery {
    #[serde(default)]
    pub workspace_id: String,
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
    pub workspace_id: String,
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
    pub agent_name: Option<String>,
    pub agent_type: Option<String>,
    pub session_id: Option<String>,
    // LLM fields
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_response_model: Option<String>,
    pub llm_input: Option<String>,
    pub llm_output: Option<String>,
    pub prompt_tokens: Option<i32>,
    pub completion_tokens: Option<i32>,
    pub total_tokens: Option<i32>,
    pub prompt_cost_usd: Option<f64>,
    pub completion_cost_usd: Option<f64>,
    pub total_cost_usd: Option<f64>,
    // Tool fields
    pub tool_name: Option<String>,
    pub tool_call_id: Option<String>,
    // Guardrails fields
    pub rail_type: Option<String>,
    pub rail_name: Option<String>,
    pub rail_stop: Option<bool>,
    pub action_name: Option<String>,
    // NeMo / GenAI 1.29 fields
    pub workflow_run_id: Option<String>,
    pub framework: Option<String>,
    // Phase 4 R4.2 / R4.3 — NeMo extensions on Guardrails LLM children.
    pub llm_cache_hit: Option<bool>,
    pub llm_response_id: Option<String>,
    // Phase 4 R4.5 — deployment.environment resource attribute (prod / staging / etc.).
    pub environment: Option<String>,
    // Database Phase 4 Gap #46
    pub db_system_name: Option<String>,
    pub db_namespace: Option<String>,
    pub db_operation_name: Option<String>,
    pub db_query_text: Option<String>,
    pub db_query_summary: Option<String>,
    pub db_collection_name: Option<String>,
    pub db_response_status_code: Option<String>,
    // Agentic semantic conventions (OTel GenAI SIG)
    pub agent_id: Option<String>,
    pub agent_description: Option<String>,
    pub agent_task_id: Option<String>,
    pub agent_task_parent_id: Option<String>,
    pub agent_task_name: Option<String>,
    pub agent_task_kind: Option<String>,
    pub agent_task_state: Option<String>,
    pub agent_task_status: Option<String>,
    pub memory_type: Option<String>,
    pub memory_key: Option<String>,
    // MCP fields
    pub mcp_tool_name: Option<String>,
    pub mcp_server_name: Option<String>,
    pub mcp_tool_input: Option<String>,
    pub mcp_tool_output: Option<String>,
    // Versioning
    pub agent_version: Option<String>,
    pub sdk_version: Option<String>,
    // Phase 4 R4.4 — gen_ai.request.* sampling params parsed back from the
    // model_parameters JSON column. None when no allowlisted params present.
    pub model_parameters: Option<serde_json::Value>,
    // Span events (serialized JSON allowlist)
    pub events: Option<serde_json::Value>,
    // OTLP span links (Phase 4 R4.6). Serialized JSON parsed back here.
    pub links: Option<serde_json::Value>,
    pub attributes: HashMap<String, serde_json::Value>,
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

#[derive(Debug, Serialize, ToSchema)]
pub struct DatabaseAnalytics {
    pub db_system_name: String,
    pub db_operation_name: Option<String>,
    pub request_count: i64,
    pub avg_duration_ms: f64,
    pub error_count: i64,
}

/// Per-rail-type breakdown for guardrails analytics.
#[derive(Debug, Serialize, ToSchema)]
pub struct RailTypeBreakdownDto {
    pub rail_type: String,
    pub count: i64,
    pub halted: i64,
    pub halt_rate: f64,
}

/// Per-rail-name stat for guardrails analytics.
#[derive(Debug, Serialize, ToSchema)]
pub struct RailNameStatDto {
    pub rail_name: String,
    pub rail_type: String,
    pub halts: i64,
    pub total: i64,
}

/// Guardrails analytics response (R2.2).
#[derive(Debug, Serialize, ToSchema)]
pub struct GuardrailsAnalytics {
    pub total_requests: i64,
    pub halted_requests: i64,
    pub halt_rate: f64,
    pub by_rail_type: Vec<RailTypeBreakdownDto>,
    pub top_halting_rails: Vec<RailNameStatDto>,
}

/// Storage usage query filters.
#[derive(Debug, Deserialize, ToSchema)]
pub struct StorageUsageQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub signal_type: Option<String>,
    pub location: Option<String>,
}

/// Aggregated storage usage for active telemetry files.
#[derive(Debug, Serialize, ToSchema)]
pub struct StorageUsage {
    pub workspace_id: String,
    pub signal_type: String,
    pub location: String,
    pub file_count: i64,
    pub records: i64,
    pub original_size: i64,
    pub compressed_size: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct StorageUsageDailyQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub signal: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StorageUsageDaily {
    pub workspace_id: String,
    pub signal: String,
    pub day: String,
    pub compressed_bytes: i64,
    pub file_count: i64,
    pub captured_at: i64,
    pub estimated_today: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct QuotaStatusQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub signal: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UsageDailyQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub signal: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct IngestRateQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub signal: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryUsageQuery {
    #[serde(default)]
    pub workspace_id: String,
    pub signal: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
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
    pub workspace_id: String,
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
    pub workspace_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub severity: Option<String>,
    pub service_name: Option<String>,
    pub trace_id: Option<String>,
    pub search_text: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Metric query filters (API-level)
#[derive(Debug, Deserialize, ToSchema)]
pub struct MetricQueryFilters {
    #[serde(default)]
    pub workspace_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub metric_name: Option<String>,
    pub service_name: Option<String>,
    pub agent_name: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Metric time-series filters (API-level)
#[derive(Debug, Deserialize, ToSchema)]
pub struct MetricSeriesFilters {
    #[serde(default)]
    pub workspace_id: String,
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
