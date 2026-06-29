//! Proto ↔ HTTP DTO conversion helpers for gRPC handlers.

use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use tonic::Status;
use zradar_models::{AuditLog, WorkspaceId, WorkspaceSettings};
use zradar_policy::{
    Operation, Policy, PolicyLimit, PolicySource, QuotaStatus, ResolvedPolicy, SignalKind,
    ThresholdStatus,
};
use zradar_retention::RetentionRunStats;

use crate::errors::ControlError;
use crate::telemetry::types::{
    AgentAnalytics, AnalyticsQuery, AnalyticsResult, ErrorAnalyticsQuery, ErrorBreakdown,
    GuardrailsAnalytics, IngestRateQuery, LlmAnalytics, LogDetail, LogQueryFilters, MetricDetail,
    MetricQueryFilters, MetricSeriesFilters, MetricSeriesPoint, PaginatedResponse, QueryUsageQuery,
    QuotaStatusQuery, SpanDetail, SpanQueryFilters, StorageUsage, StorageUsageDaily,
    StorageUsageDailyQuery, StorageUsageQuery, TopEndpoint, TopNQuery, TraceDetail,
    TraceQueryFilters, TraceSummary, UsageDailyQuery,
};

use super::admin_proto;
use super::query_proto;

// ── Common helpers ──────────────────────────────────────────────────

pub fn parse_workspace_id(s: &str) -> Result<WorkspaceId, Status> {
    WorkspaceId::from_str(s)
        .map_err(|_| Status::invalid_argument(format!("invalid workspace_id: {s}")))
}

pub fn proto_time_range(
    tr: &query_proto::TimeRange,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    (
        tr.start.as_ref().and_then(proto_timestamp_to_datetime),
        tr.end.as_ref().and_then(proto_timestamp_to_datetime),
    )
}

pub fn datetime_to_proto(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

pub fn proto_timestamp_to_datetime(ts: &prost_types::Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
}

pub fn optional_proto_timestamp(ts: &Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    ts.as_ref().and_then(proto_timestamp_to_datetime)
}

pub fn map_control_error(e: ControlError) -> Status {
    match e {
        ControlError::Database(err) => Status::internal(format!("Database error: {err}")),
        ControlError::NotFound(msg) => Status::not_found(msg),
        ControlError::Unauthorized(msg) => Status::unauthenticated(msg),
        ControlError::Forbidden(msg) => Status::permission_denied(msg),
        ControlError::InvalidInput(msg) => Status::invalid_argument(msg),
        ControlError::Internal(msg) => Status::internal(msg),
    }
}

pub fn map_anyhow_error(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

fn pagination_limit_offset(pagination: &Option<query_proto::PaginationRequest>) -> (i64, i32) {
    match pagination {
        Some(p) => {
            let limit = if p.limit > 0 { p.limit as i64 } else { 100 };
            (limit, p.offset)
        }
        None => (100, 0),
    }
}

fn json_map_to_proto(attrs: &HashMap<String, serde_json::Value>) -> HashMap<String, String> {
    attrs
        .iter()
        .map(|(k, v)| {
            let val_str = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            (k.clone(), val_str)
        })
        .collect()
}

fn optional_json_string(value: &Option<serde_json::Value>) -> Option<String> {
    value
        .as_ref()
        .map(|v| serde_json::to_string(v).unwrap_or_default())
}

// ── Request → filter converters ─────────────────────────────────────

pub fn trace_filters_from_request(req: &query_proto::QueryTracesRequest) -> TraceQueryFilters {
    let (start_time, end_time) = req
        .time_range
        .as_ref()
        .map(proto_time_range)
        .unwrap_or((None, None));
    let (limit, offset) = pagination_limit_offset(&req.pagination);

    TraceQueryFilters {
        workspace_id: req.workspace_id.clone(),
        start_time,
        end_time,
        service_name: req.service_name.clone(),
        operation_name: req.operation_name.clone(),
        min_duration_ms: req
            .min_duration_ms
            .map(|d| i64::try_from(d).unwrap_or(i64::MAX)),
        max_duration_ms: req
            .max_duration_ms
            .map(|d| i64::try_from(d).unwrap_or(i64::MAX)),
        status: req.status.clone(),
        llm_model: req.llm_model.clone(),
        llm_provider: req.llm_provider.clone(),
        llm_response_model: req.llm_response_model.clone(),
        agent_name: req.agent_name.clone(),
        session_id: req.session_id.clone(),
        rail_type: req.rail_type.clone(),
        action_name: req.action_name.clone(),
        workflow_run_id: req.workflow_run_id.clone(),
        framework: req.framework.clone(),
        tool_name: req.tool_name.clone(),
        invocation_id: req.invocation_id.clone(),
        environment: req.environment.clone(),
        limit: Some(limit),
        offset: Some(offset as i64),
    }
}

pub fn span_filters_from_request(req: &query_proto::QuerySpansRequest) -> SpanQueryFilters {
    let (start_time, end_time) = req
        .time_range
        .as_ref()
        .map(proto_time_range)
        .unwrap_or((None, None));
    let (limit, offset) = pagination_limit_offset(&req.pagination);
    let span_types = if req.span_types.is_empty() {
        None
    } else {
        Some(req.span_types.join(","))
    };

    SpanQueryFilters {
        workspace_id: req.workspace_id.clone(),
        trace_id: req.trace_id.clone(),
        start_time,
        end_time,
        service_name: req.service_name.clone(),
        operation_name: req.operation_name.clone(),
        span_type: None,
        span_types,
        status: req.status.clone(),
        llm_model: req.llm_model.clone(),
        llm_provider: req.llm_provider.clone(),
        llm_response_model: req.llm_response_model.clone(),
        agent_name: req.agent_name.clone(),
        session_id: req.session_id.clone(),
        rail_type: req.rail_type.clone(),
        action_name: req.action_name.clone(),
        workflow_run_id: req.workflow_run_id.clone(),
        framework: req.framework.clone(),
        tool_name: req.tool_name.clone(),
        invocation_id: req.invocation_id.clone(),
        environment: req.environment.clone(),
        limit: Some(limit),
        offset: Some(offset as i64),
    }
}

pub fn log_filters_from_request(req: &query_proto::QueryLogsRequest) -> LogQueryFilters {
    let (start_time, end_time) = req
        .time_range
        .as_ref()
        .map(proto_time_range)
        .unwrap_or((None, None));
    let (limit, offset) = pagination_limit_offset(&req.pagination);

    LogQueryFilters {
        workspace_id: req.workspace_id.clone(),
        start_time,
        end_time,
        severity: req.severity.clone(),
        service_name: req.service_name.clone(),
        trace_id: req.trace_id.clone(),
        search_text: req.search_text.clone(),
        agent_name: req.agent_name.clone(),
        session_id: req.session_id.clone(),
        limit: Some(limit),
        offset: Some(offset as i64),
    }
}

pub fn metric_filters_from_request(req: &query_proto::QueryMetricsRequest) -> MetricQueryFilters {
    let (start_time, end_time) = req
        .time_range
        .as_ref()
        .map(proto_time_range)
        .unwrap_or((None, None));
    let (limit, offset) = pagination_limit_offset(&req.pagination);

    MetricQueryFilters {
        workspace_id: req.workspace_id.clone(),
        start_time,
        end_time,
        metric_name: req.metric_name.clone(),
        service_name: req.service_name.clone(),
        agent_name: req.agent_name.clone(),
        limit: Some(limit),
        offset: Some(offset as i64),
    }
}

pub fn metric_series_filters_from_request(
    req: &query_proto::QueryMetricSeriesRequest,
) -> MetricSeriesFilters {
    let (start_time, end_time) = req
        .time_range
        .as_ref()
        .map(proto_time_range)
        .unwrap_or((None, None));

    MetricSeriesFilters {
        workspace_id: req.workspace_id.clone(),
        metric_name: req.metric_name.clone(),
        start_time,
        end_time,
        interval_seconds: req.interval_seconds,
        aggregation: req.aggregation.clone(),
        service_name: req.service_name.clone(),
    }
}

pub fn analytics_query_from_get_analytics(
    req: &query_proto::GetAnalyticsRequest,
) -> AnalyticsQuery {
    analytics_query_from_fields(
        &req.workspace_id,
        &req.start,
        &req.end,
        req.metric.clone(),
        &req.group_by,
        &req.filters,
    )
}

pub fn analytics_query_from_llm(req: &query_proto::GetLlmAnalyticsRequest) -> AnalyticsQuery {
    analytics_query_from_fields(
        &req.workspace_id,
        &req.start,
        &req.end,
        req.metric.clone(),
        &req.group_by,
        &req.filters,
    )
}

pub fn analytics_query_from_agent(req: &query_proto::GetAgentAnalyticsRequest) -> AnalyticsQuery {
    analytics_query_from_fields(
        &req.workspace_id,
        &req.start,
        &req.end,
        req.metric.clone(),
        &req.group_by,
        &req.filters,
    )
}

fn analytics_query_from_fields(
    workspace_id: &str,
    start: &Option<prost_types::Timestamp>,
    end: &Option<prost_types::Timestamp>,
    metric: Option<String>,
    group_by: &[String],
    filters: &HashMap<String, String>,
) -> AnalyticsQuery {
    AnalyticsQuery {
        workspace_id: workspace_id.to_string(),
        start: optional_proto_timestamp(start),
        end: optional_proto_timestamp(end),
        metric,
        group_by: if group_by.is_empty() {
            None
        } else {
            Some(group_by.join(","))
        },
        filters: if filters.is_empty() {
            None
        } else {
            Some(filters.clone())
        },
    }
}

pub fn analytics_query_from_metrics_summary(
    req: &query_proto::GetMetricsSummaryRequest,
) -> AnalyticsQuery {
    AnalyticsQuery {
        workspace_id: req.workspace_id.clone(),
        start: optional_proto_timestamp(&req.start),
        end: optional_proto_timestamp(&req.end),
        metric: None,
        group_by: None,
        filters: None,
    }
}

pub fn top_n_query_from_request(
    req: &query_proto::GetTopEndpointsRequest,
) -> Result<TopNQuery, Status> {
    let start_time = req
        .start_time
        .as_ref()
        .and_then(proto_timestamp_to_datetime)
        .ok_or_else(|| Status::invalid_argument("start_time is required"))?;
    let end_time = req
        .end_time
        .as_ref()
        .and_then(proto_timestamp_to_datetime)
        .ok_or_else(|| Status::invalid_argument("end_time is required"))?;

    Ok(TopNQuery {
        workspace_id: req.workspace_id.clone(),
        start_time,
        end_time,
        n: req.n,
    })
}

pub fn error_analytics_from_request(
    req: &query_proto::GetErrorBreakdownRequest,
) -> Result<ErrorAnalyticsQuery, Status> {
    let start_time = req
        .start_time
        .as_ref()
        .and_then(proto_timestamp_to_datetime)
        .ok_or_else(|| Status::invalid_argument("start_time is required"))?;
    let end_time = req
        .end_time
        .as_ref()
        .and_then(proto_timestamp_to_datetime)
        .ok_or_else(|| Status::invalid_argument("end_time is required"))?;

    Ok(ErrorAnalyticsQuery {
        workspace_id: req.workspace_id.clone(),
        start_time,
        end_time,
        service_name: req.service_name.clone(),
    })
}

pub fn guardrails_analytics_query(
    req: &query_proto::GetGuardrailsAnalyticsRequest,
) -> AnalyticsQuery {
    AnalyticsQuery {
        workspace_id: req.workspace_id.clone(),
        start: optional_proto_timestamp(&req.start),
        end: optional_proto_timestamp(&req.end),
        metric: None,
        group_by: None,
        filters: None,
    }
}

pub fn storage_usage_query_from_request(
    req: &query_proto::GetStorageUsageRequest,
) -> StorageUsageQuery {
    StorageUsageQuery {
        workspace_id: req.workspace_id.clone(),
        start_time: optional_proto_timestamp(&req.start_time),
        end_time: optional_proto_timestamp(&req.end_time),
        signal_type: req.signal_type.clone(),
        location: req.location.clone(),
    }
}

pub fn storage_usage_daily_query_from_request(
    req: &query_proto::GetStorageUsageDailyRequest,
) -> StorageUsageDailyQuery {
    StorageUsageDailyQuery {
        workspace_id: req.workspace_id.clone(),
        signal: req.signal.clone(),
        start_time: optional_proto_timestamp(&req.start_time),
        end_time: optional_proto_timestamp(&req.end_time),
    }
}

pub fn quota_status_query_from_request(
    req: &query_proto::GetQuotaStatusRequest,
) -> QuotaStatusQuery {
    QuotaStatusQuery {
        workspace_id: req.workspace_id.clone(),
        signal: req.signal.clone(),
    }
}

pub fn usage_daily_query_from_request(req: &query_proto::GetUsageDailyRequest) -> UsageDailyQuery {
    UsageDailyQuery {
        workspace_id: req.workspace_id.clone(),
        signal: req.signal.clone(),
        start_time: optional_proto_timestamp(&req.start_time),
        end_time: optional_proto_timestamp(&req.end_time),
    }
}

pub fn ingest_rate_query_from_request(req: &query_proto::GetIngestRateRequest) -> IngestRateQuery {
    IngestRateQuery {
        workspace_id: req.workspace_id.clone(),
        signal: req.signal.clone(),
        start_time: optional_proto_timestamp(&req.start_time),
        end_time: optional_proto_timestamp(&req.end_time),
    }
}

pub fn query_usage_query_from_request(req: &query_proto::GetQueryUsageRequest) -> QueryUsageQuery {
    QueryUsageQuery {
        workspace_id: req.workspace_id.clone(),
        signal: req.signal.clone(),
        start_time: optional_proto_timestamp(&req.start_time),
        end_time: optional_proto_timestamp(&req.end_time),
    }
}

// ── Response converters ─────────────────────────────────────────────

pub fn trace_summary_to_proto(t: &TraceSummary) -> query_proto::TraceSummary {
    query_proto::TraceSummary {
        trace_id: t.trace_id.clone(),
        start_time: Some(datetime_to_proto(t.start_time)),
        duration_ms: t.duration_ms,
        service_name: t.service_name.clone(),
        operation_name: t.operation_name.clone(),
        status: t.status.clone(),
        span_count: t.span_count,
    }
}

pub fn span_detail_to_proto(s: &SpanDetail) -> query_proto::SpanDetail {
    query_proto::SpanDetail {
        span_id: s.span_id.clone(),
        trace_id: s.trace_id.clone(),
        parent_span_id: s.parent_span_id.clone(),
        service_name: s.service_name.clone(),
        operation_name: s.operation_name.clone(),
        span_type: s.span_type.clone(),
        start_time: Some(datetime_to_proto(s.start_time)),
        duration_ms: s.duration_ms,
        status: s.status.clone(),
        agent_name: s.agent_name.clone(),
        agent_type: s.agent_type.clone(),
        session_id: s.session_id.clone(),
        llm_model: s.llm_model.clone(),
        llm_provider: s.llm_provider.clone(),
        llm_response_model: s.llm_response_model.clone(),
        llm_input: s.llm_input.clone(),
        llm_output: s.llm_output.clone(),
        prompt_tokens: s.prompt_tokens,
        completion_tokens: s.completion_tokens,
        total_tokens: s.total_tokens,
        prompt_cost_usd: s.prompt_cost_usd,
        completion_cost_usd: s.completion_cost_usd,
        total_cost_usd: s.total_cost_usd,
        tool_name: s.tool_name.clone(),
        tool_call_id: s.tool_call_id.clone(),
        rail_type: s.rail_type.clone(),
        rail_name: s.rail_name.clone(),
        rail_stop: s.rail_stop,
        action_name: s.action_name.clone(),
        workflow_run_id: s.workflow_run_id.clone(),
        framework: s.framework.clone(),
        llm_cache_hit: s.llm_cache_hit,
        llm_response_id: s.llm_response_id.clone(),
        environment: s.environment.clone(),
        model_parameters_json: optional_json_string(&s.model_parameters),
        events_json: optional_json_string(&s.events),
        links_json: optional_json_string(&s.links),
        attributes: json_map_to_proto(&s.attributes),
    }
}

pub fn trace_detail_to_proto(t: &TraceDetail) -> query_proto::TraceDetail {
    query_proto::TraceDetail {
        trace_id: t.trace_id.clone(),
        start_time: Some(datetime_to_proto(t.start_time)),
        duration_ms: t.duration_ms,
        spans: t.spans.iter().map(span_detail_to_proto).collect(),
    }
}

pub fn log_detail_to_proto(l: &LogDetail) -> query_proto::LogDetail {
    query_proto::LogDetail {
        id: l.id.clone(),
        timestamp: Some(datetime_to_proto(l.timestamp)),
        severity: l.severity.clone(),
        service_name: l.service_name.clone(),
        message: l.message.clone(),
        trace_id: l.trace_id.clone(),
        span_id: l.span_id.clone(),
        agent_name: l.agent_name.clone(),
        session_id: l.session_id.clone(),
        user_id: l.user_id.clone(),
        attributes: json_map_to_proto(&l.attributes),
    }
}

pub fn metric_detail_to_proto(m: &MetricDetail) -> query_proto::MetricDetail {
    query_proto::MetricDetail {
        metric_name: m.metric_name.clone(),
        metric_type: m.metric_type.clone(),
        timestamp: Some(datetime_to_proto(m.timestamp)),
        service_name: m.service_name.clone(),
        value: m.value,
        count: m.count,
        sum: m.sum,
        min: m.min,
        max: m.max,
        labels: json_map_to_proto(&m.labels),
    }
}

pub fn metric_series_point_to_proto(p: &MetricSeriesPoint) -> query_proto::MetricSeriesPoint {
    query_proto::MetricSeriesPoint {
        timestamp: Some(datetime_to_proto(p.timestamp)),
        value: p.value,
    }
}

pub fn paginated_traces_to_proto(
    page: PaginatedResponse<TraceSummary>,
    offset: i32,
) -> query_proto::QueryTracesResponse {
    query_proto::QueryTracesResponse {
        items: page.items.iter().map(trace_summary_to_proto).collect(),
        total_count: page.total,
        limit: page.page_size as i32,
        offset,
    }
}

pub fn paginated_spans_to_proto(
    page: PaginatedResponse<SpanDetail>,
    offset: i32,
) -> query_proto::QuerySpansResponse {
    query_proto::QuerySpansResponse {
        items: page.items.iter().map(span_detail_to_proto).collect(),
        total_count: page.total,
        limit: page.page_size as i32,
        offset,
    }
}

pub fn paginated_logs_to_proto(
    page: PaginatedResponse<LogDetail>,
    offset: i32,
) -> query_proto::QueryLogsResponse {
    query_proto::QueryLogsResponse {
        items: page.items.iter().map(log_detail_to_proto).collect(),
        total_count: page.total,
        limit: page.page_size as i32,
        offset,
    }
}

pub fn paginated_metrics_to_proto(
    page: PaginatedResponse<MetricDetail>,
    offset: i32,
) -> query_proto::QueryMetricsResponse {
    query_proto::QueryMetricsResponse {
        items: page.items.iter().map(metric_detail_to_proto).collect(),
        total_count: page.total,
        limit: page.page_size as i32,
        offset,
    }
}

pub fn analytics_result_to_proto(r: &AnalyticsResult) -> query_proto::AnalyticsResult {
    query_proto::AnalyticsResult {
        timestamp: r.timestamp.clone(),
        value: r.value,
        groups: r.groups.clone().unwrap_or_default(),
    }
}

pub fn metrics_summary_to_proto(
    m: &crate::telemetry::types::MetricsSummary,
) -> query_proto::GetMetricsSummaryResponse {
    query_proto::GetMetricsSummaryResponse {
        total_traces: m.total_traces,
        error_rate: m.error_rate,
        p50_latency: m.p50_latency,
        p90_latency: m.p90_latency,
        p99_latency: m.p99_latency,
    }
}

pub fn top_endpoint_to_proto(e: &TopEndpoint) -> query_proto::TopEndpoint {
    query_proto::TopEndpoint {
        operation_name: e.operation_name.clone(),
        service_name: e.service_name.clone(),
        count: e.count,
        avg_duration_ms: e.avg_duration_ms,
        p95_duration_ms: e.p95_duration_ms,
        error_rate: e.error_rate,
    }
}

pub fn error_breakdown_to_proto(e: &ErrorBreakdown) -> query_proto::ErrorBreakdown {
    query_proto::ErrorBreakdown {
        error_type: e.error_type.clone(),
        count: e.count,
        percentage: e.percentage,
    }
}

pub fn llm_analytics_to_proto(a: &LlmAnalytics) -> query_proto::LlmAnalytics {
    query_proto::LlmAnalytics {
        llm_model: a.llm_model.clone(),
        request_count: a.request_count,
        total_tokens: a.total_tokens,
        total_cost_usd: a.total_cost_usd,
        avg_duration_ms: a.avg_duration_ms,
    }
}

pub fn agent_analytics_to_proto(a: &AgentAnalytics) -> query_proto::AgentAnalytics {
    query_proto::AgentAnalytics {
        agent_name: a.agent_name.clone(),
        agent_type: a.agent_type.clone(),
        span_count: a.span_count,
        error_count: a.error_count,
        total_tokens: a.total_tokens,
        avg_duration_ms: a.avg_duration_ms,
    }
}

pub fn guardrails_analytics_to_proto(
    g: &GuardrailsAnalytics,
) -> query_proto::GetGuardrailsAnalyticsResponse {
    query_proto::GetGuardrailsAnalyticsResponse {
        total_requests: g.total_requests,
        halted_requests: g.halted_requests,
        halt_rate: g.halt_rate,
        by_rail_type: g
            .by_rail_type
            .iter()
            .map(|r| query_proto::RailTypeBreakdown {
                rail_type: r.rail_type.clone(),
                count: r.count,
                halted: r.halted,
                halt_rate: r.halt_rate,
            })
            .collect(),
        top_halting_rails: g
            .top_halting_rails
            .iter()
            .map(|r| query_proto::RailNameStat {
                rail_name: r.rail_name.clone(),
                rail_type: r.rail_type.clone(),
                halts: r.halts,
                total: r.total,
            })
            .collect(),
    }
}

pub fn storage_usage_to_proto(u: &StorageUsage) -> query_proto::StorageUsage {
    query_proto::StorageUsage {
        workspace_id: u.workspace_id.clone(),
        signal_type: u.signal_type.clone(),
        location: u.location.clone(),
        file_count: u.file_count,
        records: u.records,
        original_size: u.original_size,
        compressed_size: u.compressed_size,
    }
}

pub fn storage_usage_daily_to_proto(d: &StorageUsageDaily) -> query_proto::StorageUsageDaily {
    query_proto::StorageUsageDaily {
        workspace_id: d.workspace_id.clone(),
        signal: d.signal.clone(),
        day: d.day.clone(),
        compressed_bytes: d.compressed_bytes,
        file_count: d.file_count,
        captured_at: d.captured_at,
        estimated_today: d.estimated_today,
    }
}

fn threshold_status_to_str(s: ThresholdStatus) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "ok".to_string())
}

fn signal_kind_to_str(s: SignalKind) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn operation_to_str(o: Operation) -> String {
    serde_json::to_value(o)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

pub fn quota_status_to_proto(q: &QuotaStatus) -> query_proto::QuotaStatus {
    query_proto::QuotaStatus {
        signal: signal_kind_to_str(q.signal),
        operation: operation_to_str(q.operation),
        limit_kind: q.limit_kind.clone(),
        limit_value: q.limit_value,
        observed_value: q.observed_value,
        pct_consumed: q.pct_consumed,
        status: threshold_status_to_str(q.status),
        period_start: q.period_start,
        period_end: q.period_end,
        projected_exhaustion_at: q.projected_exhaustion_at,
    }
}

pub fn usage_daily_to_proto(r: &zradar_policy::UsageDailyRecord) -> query_proto::UsageDailyRecord {
    query_proto::UsageDailyRecord {
        workspace_id: r.workspace_id.to_string(),
        signal: signal_kind_to_str(r.signal),
        operation: operation_to_str(r.operation),
        day: r.day.clone(),
        used_bytes: r.used_bytes,
        records: r.records,
        query_count: r.query_count,
        file_count: r.file_count,
    }
}

pub fn ingest_rate_to_proto(r: &zradar_policy::IngestRateRecord) -> query_proto::IngestRateRecord {
    query_proto::IngestRateRecord {
        workspace_id: r.workspace_id.to_string(),
        signal: signal_kind_to_str(r.signal),
        records_per_sec: r.records_per_sec,
        bytes_per_sec: r.bytes_per_sec,
        window_start_micros: r.window_start_micros,
        window_end_micros: r.window_end_micros,
    }
}

pub fn query_usage_to_proto(r: &zradar_policy::QueryUsageRecord) -> query_proto::QueryUsageRecord {
    query_proto::QueryUsageRecord {
        workspace_id: r.workspace_id.to_string(),
        signal: signal_kind_to_str(r.signal),
        bytes_scanned: r.bytes_scanned,
        rows_scanned: r.rows_scanned,
        query_count: r.query_count,
        avg_query_time_ms: r.avg_query_time_ms,
        window_start_micros: r.window_start_micros,
        window_end_micros: r.window_end_micros,
    }
}

// ── Admin proto converters ──────────────────────────────────────────

pub fn retention_run_stats_to_proto(stats: &RetentionRunStats) -> admin_proto::RetentionRunStats {
    admin_proto::RetentionRunStats {
        files_marked: stats.files_marked,
        files_deleted: stats.files_deleted,
        bytes_freed: stats.bytes_freed,
        files_skipped_leased: stats.files_skipped_leased,
        projects_processed: stats.projects_processed,
        errors: stats.errors.clone(),
        duration_ms: stats.duration_ms,
    }
}

pub fn policy_to_proto(p: &Policy) -> admin_proto::Policy {
    admin_proto::Policy {
        id: p.id.map(|id| id.0),
        workspace_id: p.workspace_id.to_string(),
        signal: signal_kind_to_str(p.signal),
        operation: operation_to_str(p.operation),
        limit_json: serde_json::to_string(&p.limit).unwrap_or_default(),
        grace_pct: p.grace_pct as u32,
        hard_block_pct: p.hard_block_pct as u32,
        effective_from: p.effective_from,
        effective_until: p.effective_until,
        source: policy_source_to_str(p.source),
    }
}

fn policy_source_to_str(s: PolicySource) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "api".to_string())
}

pub fn resolved_policy_to_proto(r: &ResolvedPolicy) -> admin_proto::ResolvedPolicy {
    admin_proto::ResolvedPolicy {
        blocked: r.blocked,
        rate_json: r
            .rate
            .as_ref()
            .map(|l| serde_json::to_string(l).unwrap_or_default()),
        quotas_json: r
            .quotas
            .iter()
            .map(|l| serde_json::to_string(l).unwrap_or_default())
            .collect(),
        size_json: r
            .size
            .as_ref()
            .map(|l| serde_json::to_string(l).unwrap_or_default()),
        retention_json: r
            .retention
            .as_ref()
            .map(|l| serde_json::to_string(l).unwrap_or_default()),
        query_window_json: r
            .query_window
            .as_ref()
            .map(|l| serde_json::to_string(l).unwrap_or_default()),
        grace_pct: r.grace_pct as u32,
        hard_block_pct: r.hard_block_pct as u32,
    }
}

pub fn parse_signal_kind(s: &str) -> Result<SignalKind, Status> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| Status::invalid_argument(format!("invalid signal: {s}")))
}

pub fn parse_operation(s: &str) -> Result<Operation, Status> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| Status::invalid_argument(format!("invalid operation: {s}")))
}

pub fn parse_policy_limit(json: &str) -> Result<PolicyLimit, Status> {
    serde_json::from_str(json)
        .map_err(|e| Status::invalid_argument(format!("invalid limit_json: {e}")))
}

pub fn parse_policy_source(s: &str) -> Result<PolicySource, Status> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| Status::invalid_argument(format!("invalid source: {s}")))
}

pub fn policy_config_to_policy(
    workspace_id: WorkspaceId,
    cfg: &admin_proto::PolicyConfig,
) -> Result<Policy, Status> {
    let now = chrono::Utc::now().timestamp_micros();
    Ok(Policy {
        id: None,
        workspace_id,
        signal: parse_signal_kind(&cfg.signal)?,
        operation: parse_operation(&cfg.operation)?,
        limit: parse_policy_limit(&cfg.limit_json)?,
        grace_pct: cfg.grace_pct.unwrap_or(101).min(255) as u8,
        hard_block_pct: cfg.hard_block_pct.unwrap_or(103).min(255) as u8,
        effective_from: cfg.effective_from.unwrap_or(now),
        effective_until: cfg.effective_until,
        source: cfg
            .source
            .as_deref()
            .map(parse_policy_source)
            .transpose()?
            .unwrap_or(PolicySource::Api),
    })
}

pub fn audit_log_to_proto(log: &AuditLog) -> admin_proto::AuditLog {
    admin_proto::AuditLog {
        id: log.id,
        actor_workspace_id: log.actor_workspace_id.map(|id| id.to_string()),
        resource_workspace_id: log.resource_workspace_id.map(|id| id.to_string()),
        action: log.action.clone(),
        resource_type: log.resource_type.clone(),
        resource_id: log.resource_id.clone(),
        metadata_json: log.metadata.to_string(),
        created_at: log.created_at,
    }
}

pub fn workspace_settings_to_proto(s: &WorkspaceSettings) -> admin_proto::WorkspaceSettings {
    admin_proto::WorkspaceSettings {
        id: s.id,
        workspace_id: s.workspace_id.to_string(),
        traces_retention_days: s.traces_retention_days,
        metrics_retention_days: s.metrics_retention_days,
        logs_retention_days: s.logs_retention_days,
        max_ingestion_rate: s.max_ingestion_rate,
        file_push_interval_secs: s.file_push_interval_secs,
        blocked: s.blocked,
        capture_llm_content_enabled: s.capture_llm_content_enabled,
        updated_at: s.updated_at,
    }
}
