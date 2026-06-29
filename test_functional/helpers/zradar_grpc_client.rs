//! gRPC clients for zradar Query (:8081) and Admin (:8082) APIs.

use std::collections::HashMap;

use anyhow::{Context, Result};
use api::grpc::admin_proto::audit_service_client::AuditServiceClient;
use api::grpc::admin_proto::policy_service_client::PolicyServiceClient;
use api::grpc::admin_proto::retention_service_client::RetentionServiceClient;
use api::grpc::admin_proto::settings_service_client::SettingsServiceClient;
use api::grpc::admin_proto::{
    GetEffectivePolicyRequest, GetEffectivePolicyResponse, GetWorkspaceRetentionRequest,
    GetWorkspaceRetentionResponse, GetWorkspaceSettingsRequest, GetWorkspaceSettingsResponse,
    ListAuditLogsRequest, ListAuditLogsResponse, ListPoliciesRequest, ListPoliciesResponse,
    PolicyConfig, RunCleanupRequest, RunCleanupResponse, SetWorkspaceRetentionRequest,
    SetWorkspaceRetentionResponse, UpdateWorkspaceSettingsRequest, UpdateWorkspaceSettingsResponse,
    UpsertPoliciesRequest, UpsertPoliciesResponse,
};
use api::grpc::query_proto::analytics_service_client::AnalyticsServiceClient;
use api::grpc::query_proto::query_service_client::QueryServiceClient;
use api::grpc::query_proto::{
    GetAgentAnalyticsRequest, GetAgentAnalyticsResponse, GetAnalyticsRequest, GetAnalyticsResponse,
    GetErrorBreakdownRequest, GetErrorBreakdownResponse, GetGuardrailsAnalyticsRequest,
    GetGuardrailsAnalyticsResponse, GetIngestRateRequest, GetIngestRateResponse,
    GetLlmAnalyticsRequest, GetLlmAnalyticsResponse, GetLogRequest, GetLogResponse,
    GetMetricsSummaryRequest, GetMetricsSummaryResponse, GetQueryUsageRequest,
    GetQueryUsageResponse, GetQuotaStatusRequest, GetQuotaStatusResponse, GetSpanRequest,
    GetSpanResponse, GetStorageUsageDailyRequest, GetStorageUsageDailyResponse,
    GetStorageUsageRequest, GetStorageUsageResponse, GetTopEndpointsRequest,
    GetTopEndpointsResponse, GetTraceRequest, GetTraceResponse, GetUsageDailyRequest,
    GetUsageDailyResponse, PaginationRequest, QueryLogsRequest, QueryLogsResponse,
    QueryMetricSeriesRequest, QueryMetricSeriesResponse, QueryMetricsRequest, QueryMetricsResponse,
    QuerySpansRequest, QuerySpansResponse, QueryTracesRequest, QueryTracesResponse, TimeRange,
};
use chrono::{Duration, Utc};
use prost_types::Timestamp;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

macro_rules! with_query_client {
    ($auth:expr, |$client:ident| $body:expr) => {{
        let channel = connect(&$auth.url).await?;
        let api_key_token = $auth
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {key}")).ok());
        let workspace_id_val = MetadataValue::try_from($auth.workspace_id.as_str()).ok();
        let mut $client =
            QueryServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(value) = &workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", value.clone());
                }
                Ok(req)
            });
        $body
    }};
}

macro_rules! with_analytics_client {
    ($auth:expr, |$client:ident| $body:expr) => {{
        let channel = connect(&$auth.url).await?;
        let api_key_token = $auth
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {key}")).ok());
        let workspace_id_val = MetadataValue::try_from($auth.workspace_id.as_str()).ok();
        let mut $client =
            AnalyticsServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(value) = &workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", value.clone());
                }
                Ok(req)
            });
        $body
    }};
}

macro_rules! with_admin_client {
    ($auth:expr, $client_ty:ident, |$client:ident| $body:expr) => {{
        let channel = connect(&$auth.url).await?;
        let api_key_token = $auth
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {key}")).ok());
        let workspace_id_val = MetadataValue::try_from($auth.workspace_id.as_str()).ok();
        let mut $client =
            $client_ty::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(value) = &workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", value.clone());
                }
                Ok(req)
            });
        $body
    }};
}

async fn connect(url: &str) -> Result<Channel> {
    Channel::from_shared(url.to_string())
        .context("Invalid gRPC URL")?
        .connect()
        .await
        .context("Failed to connect to gRPC server")
}

fn default_pagination() -> PaginationRequest {
    PaginationRequest {
        limit: 100,
        offset: 0,
    }
}

pub fn timestamp_now() -> Timestamp {
    let now = Utc::now();
    Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    }
}

pub fn timestamp_hours_ago(hours: i64) -> Timestamp {
    let t = Utc::now() - Duration::hours(hours);
    Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}

pub fn recent_time_range() -> TimeRange {
    TimeRange {
        start: Some(timestamp_hours_ago(1)),
        end: Some(timestamp_now()),
    }
}

pub fn extended_time_range() -> TimeRange {
    TimeRange {
        start: Some(timestamp_hours_ago(4)),
        end: Some(timestamp_now()),
    }
}

/// Returns true when a gRPC error indicates data is not yet queryable (retry polling).
pub fn grpc_not_ready(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::NotFound)
    })
}

/// Span query parameters for gRPC QuerySpans RPC.
#[derive(Debug, Clone, Default)]
pub struct SpanQueryParams {
    pub trace_id: Option<String>,
    pub time_range: Option<TimeRange>,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub span_types: Vec<String>,
    pub status: Option<String>,
    pub llm_model: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_response_model: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub rail_type: Option<String>,
    pub action_name: Option<String>,
    pub workflow_run_id: Option<String>,
    pub framework: Option<String>,
    pub tool_name: Option<String>,
    pub invocation_id: Option<String>,
    pub environment: Option<String>,
}

/// Pair of authenticated gRPC clients for functional tests.
pub struct ZradarGrpcClients {
    pub query: ZradarQueryClient,
    pub admin: ZradarAdminClient,
}

impl ZradarGrpcClients {
    pub fn from_test_env(
        query_grpc_url: String,
        admin_grpc_url: String,
        api_key: String,
        workspace_id: String,
    ) -> Self {
        Self {
            query: ZradarQueryClient::new(query_grpc_url)
                .with_api_key(api_key.clone())
                .with_workspace_id(workspace_id.clone()),
            admin: ZradarAdminClient::new(admin_grpc_url)
                .with_api_key(api_key)
                .with_workspace_id(workspace_id),
        }
    }
}

#[derive(Clone)]
struct GrpcAuth {
    url: String,
    api_key: Option<String>,
    workspace_id: String,
}

/// Client for Query + Analytics gRPC APIs on port 8081.
#[derive(Clone)]
pub struct ZradarQueryClient {
    auth: GrpcAuth,
}

impl ZradarQueryClient {
    pub fn new(url: String) -> Self {
        Self {
            auth: GrpcAuth {
                url,
                api_key: None,
                workspace_id: String::new(),
            },
        }
    }

    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.auth.api_key = Some(api_key);
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: String) -> Self {
        self.auth.workspace_id = workspace_id;
        self
    }

    pub async fn query_traces(
        &self,
        operation_name: Option<&str>,
        time_range: Option<TimeRange>,
    ) -> Result<QueryTracesResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .query_traces(QueryTracesRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    time_range: time_range.or_else(|| Some(recent_time_range())),
                    service_name: None,
                    operation_name: operation_name.map(str::to_string),
                    status: None,
                    min_duration_ms: None,
                    max_duration_ms: None,
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
                    pagination: Some(default_pagination()),
                })
                .await
                .context("QueryTraces RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_trace(&self, trace_id: &str) -> Result<GetTraceResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .get_trace(GetTraceRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    trace_id: trace_id.to_string(),
                })
                .await
                .context("GetTrace RPC failed")?
                .into_inner())
        })
    }

    pub async fn query_spans(&self, trace_id: &str) -> Result<QuerySpansResponse> {
        self.query_spans_filtered(Some(trace_id), None).await
    }

    pub async fn query_spans_filtered(
        &self,
        trace_id: Option<&str>,
        operation_name: Option<&str>,
    ) -> Result<QuerySpansResponse> {
        self.query_spans_with(SpanQueryParams {
            trace_id: trace_id.map(str::to_string),
            operation_name: operation_name.map(str::to_string),
            ..SpanQueryParams::default()
        })
        .await
    }

    pub async fn query_spans_with(&self, params: SpanQueryParams) -> Result<QuerySpansResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .query_spans(QuerySpansRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    trace_id: params.trace_id,
                    time_range: params.time_range,
                    service_name: params.service_name,
                    operation_name: params.operation_name,
                    span_types: params.span_types,
                    status: params.status,
                    llm_model: params.llm_model,
                    llm_provider: params.llm_provider,
                    llm_response_model: params.llm_response_model,
                    agent_name: params.agent_name,
                    session_id: params.session_id,
                    rail_type: params.rail_type,
                    action_name: params.action_name,
                    workflow_run_id: params.workflow_run_id,
                    framework: params.framework,
                    tool_name: params.tool_name,
                    invocation_id: params.invocation_id,
                    environment: params.environment,
                    pagination: Some(default_pagination()),
                })
                .await
                .context("QuerySpans RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_span(&self, span_id: &str) -> Result<GetSpanResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .get_span(GetSpanRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    span_id: span_id.to_string(),
                })
                .await
                .context("GetSpan RPC failed")?
                .into_inner())
        })
    }

    pub async fn query_logs(&self) -> Result<QueryLogsResponse> {
        self.query_logs_filtered(None).await
    }

    pub async fn query_logs_filtered(&self, trace_id: Option<&str>) -> Result<QueryLogsResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .query_logs(QueryLogsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    time_range: Some(extended_time_range()),
                    severity: None,
                    service_name: None,
                    trace_id: trace_id.map(str::to_string),
                    search_text: None,
                    agent_name: None,
                    session_id: None,
                    pagination: Some(default_pagination()),
                })
                .await
                .context("QueryLogs RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_log(&self, log_id: &str) -> Result<GetLogResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .get_log(GetLogRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    log_id: log_id.to_string(),
                })
                .await
                .context("GetLog RPC failed")?
                .into_inner())
        })
    }

    pub async fn query_metrics(&self, metric_name: &str) -> Result<QueryMetricsResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .query_metrics(QueryMetricsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    time_range: Some(recent_time_range()),
                    // Pass None when metric_name is empty so the server returns all metrics
                    // (empty string would be treated as a filter matching no metrics)
                    metric_name: if metric_name.is_empty() {
                        None
                    } else {
                        Some(metric_name.to_string())
                    },
                    service_name: None,
                    agent_name: None,
                    pagination: Some(default_pagination()),
                })
                .await
                .context("QueryMetrics RPC failed")?
                .into_inner())
        })
    }

    pub async fn query_metric_series(
        &self,
        metric_name: &str,
    ) -> Result<QueryMetricSeriesResponse> {
        with_query_client!(self.auth, |client| {
            Ok(client
                .query_metric_series(QueryMetricSeriesRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    metric_name: metric_name.to_string(),
                    time_range: Some(recent_time_range()),
                    interval_seconds: Some(60),
                    aggregation: Some("avg".to_string()),
                    service_name: None,
                })
                .await
                .context("QueryMetricSeries RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_analytics(&self) -> Result<GetAnalyticsResponse> {
        self.get_analytics_with_params("trace_count", vec![], HashMap::new())
            .await
    }

    pub async fn get_analytics_with_params(
        &self,
        metric: &str,
        group_by: Vec<String>,
        filters: HashMap<String, String>,
    ) -> Result<GetAnalyticsResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_analytics(GetAnalyticsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start: Some(timestamp_hours_ago(1)),
                    end: Some(timestamp_now()),
                    metric: Some(metric.to_string()),
                    group_by,
                    filters,
                })
                .await
                .context("GetAnalytics RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_metrics_summary(&self) -> Result<GetMetricsSummaryResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_metrics_summary(GetMetricsSummaryRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start: Some(timestamp_hours_ago(1)),
                    end: Some(timestamp_now()),
                })
                .await
                .context("GetMetricsSummary RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_top_endpoints(&self) -> Result<GetTopEndpointsResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_top_endpoints(GetTopEndpointsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start_time: Some(timestamp_hours_ago(1)),
                    end_time: Some(timestamp_now()),
                    n: Some(5),
                })
                .await
                .context("GetTopEndpoints RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_error_breakdown(&self) -> Result<GetErrorBreakdownResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_error_breakdown(GetErrorBreakdownRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start_time: Some(timestamp_hours_ago(1)),
                    end_time: Some(timestamp_now()),
                    service_name: None,
                })
                .await
                .context("GetErrorBreakdown RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_llm_analytics(&self) -> Result<GetLlmAnalyticsResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_llm_analytics(GetLlmAnalyticsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start: Some(timestamp_hours_ago(1)),
                    end: Some(timestamp_now()),
                    metric: None,
                    group_by: vec![],
                    filters: Default::default(),
                })
                .await
                .context("GetLlmAnalytics RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_agent_analytics(&self) -> Result<GetAgentAnalyticsResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_agent_analytics(GetAgentAnalyticsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start: Some(timestamp_hours_ago(1)),
                    end: Some(timestamp_now()),
                    metric: None,
                    group_by: vec![],
                    filters: Default::default(),
                })
                .await
                .context("GetAgentAnalytics RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_guardrails_analytics(&self) -> Result<GetGuardrailsAnalyticsResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_guardrails_analytics(GetGuardrailsAnalyticsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start: Some(timestamp_hours_ago(1)),
                    end: Some(timestamp_now()),
                })
                .await
                .context("GetGuardrailsAnalytics RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_storage_usage(&self) -> Result<GetStorageUsageResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_storage_usage(GetStorageUsageRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    start_time: Some(timestamp_hours_ago(24)),
                    end_time: Some(timestamp_now()),
                    signal_type: None,
                    location: None,
                })
                .await
                .context("GetStorageUsage RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_storage_usage_daily(&self) -> Result<GetStorageUsageDailyResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_storage_usage_daily(GetStorageUsageDailyRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    signal: None,
                    start_time: Some(timestamp_hours_ago(24)),
                    end_time: Some(timestamp_now()),
                })
                .await
                .context("GetStorageUsageDaily RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_quota_status(&self) -> Result<GetQuotaStatusResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_quota_status(GetQuotaStatusRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    signal: None,
                })
                .await
                .context("GetQuotaStatus RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_usage_daily(&self) -> Result<GetUsageDailyResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_usage_daily(GetUsageDailyRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    signal: None,
                    start_time: Some(timestamp_hours_ago(24)),
                    end_time: Some(timestamp_now()),
                })
                .await
                .context("GetUsageDaily RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_ingest_rate(&self) -> Result<GetIngestRateResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_ingest_rate(GetIngestRateRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    signal: None,
                    start_time: Some(timestamp_hours_ago(1)),
                    end_time: Some(timestamp_now()),
                })
                .await
                .context("GetIngestRate RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_query_usage(&self) -> Result<GetQueryUsageResponse> {
        with_analytics_client!(self.auth, |client| {
            Ok(client
                .get_query_usage(GetQueryUsageRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    signal: None,
                    start_time: Some(timestamp_hours_ago(1)),
                    end_time: Some(timestamp_now()),
                })
                .await
                .context("GetQueryUsage RPC failed")?
                .into_inner())
        })
    }
}

#[derive(Clone)]
struct AdminAuth {
    url: String,
    api_key: Option<String>,
    workspace_id: String,
}

/// Input for updating workspace settings — groups fields to avoid exceeding clippy argument limit.
pub struct WorkspaceSettingsInput {
    pub traces_retention_days: i32,
    pub metrics_retention_days: i32,
    pub logs_retention_days: i32,
    pub max_ingestion_rate: Option<i32>,
    pub file_push_interval_secs: i32,
    pub blocked: bool,
    pub capture_llm_content_enabled: bool,
}

/// Client for Admin gRPC APIs on port 8082.
#[derive(Clone)]
pub struct ZradarAdminClient {
    auth: AdminAuth,
}

impl ZradarAdminClient {
    pub fn new(url: String) -> Self {
        Self {
            auth: AdminAuth {
                url,
                api_key: None,
                workspace_id: String::new(),
            },
        }
    }

    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.auth.api_key = Some(api_key);
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: String) -> Self {
        self.auth.workspace_id = workspace_id;
        self
    }

    pub async fn get_workspace_retention(&self) -> Result<GetWorkspaceRetentionResponse> {
        with_admin_client!(self.auth, RetentionServiceClient, |client| {
            Ok(client
                .get_workspace_retention(GetWorkspaceRetentionRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                })
                .await
                .context("GetWorkspaceRetention RPC failed")?
                .into_inner())
        })
    }

    pub async fn set_workspace_retention(
        &self,
        days: u32,
    ) -> Result<SetWorkspaceRetentionResponse> {
        with_admin_client!(self.auth, RetentionServiceClient, |client| {
            Ok(client
                .set_workspace_retention(SetWorkspaceRetentionRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    retention_days: days,
                })
                .await
                .context("SetWorkspaceRetention RPC failed")?
                .into_inner())
        })
    }

    pub async fn run_cleanup(&self) -> Result<RunCleanupResponse> {
        with_admin_client!(self.auth, RetentionServiceClient, |client| {
            Ok(client
                .run_cleanup(RunCleanupRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    retention_days: None,
                })
                .await
                .context("RunCleanup RPC failed")?
                .into_inner())
        })
    }

    pub async fn list_policies(&self) -> Result<ListPoliciesResponse> {
        with_admin_client!(self.auth, PolicyServiceClient, |client| {
            Ok(client
                .list_policies(ListPoliciesRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                })
                .await
                .context("ListPolicies RPC failed")?
                .into_inner())
        })
    }

    pub async fn upsert_policies(
        &self,
        policies: Vec<PolicyConfig>,
    ) -> Result<UpsertPoliciesResponse> {
        with_admin_client!(self.auth, PolicyServiceClient, |client| {
            Ok(client
                .upsert_policies(UpsertPoliciesRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    policies,
                })
                .await
                .context("UpsertPolicies RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_effective_policy(&self) -> Result<GetEffectivePolicyResponse> {
        with_admin_client!(self.auth, PolicyServiceClient, |client| {
            Ok(client
                .get_effective_policy(GetEffectivePolicyRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                })
                .await
                .context("GetEffectivePolicy RPC failed")?
                .into_inner())
        })
    }

    pub async fn list_audit_logs(&self, action: Option<&str>) -> Result<ListAuditLogsResponse> {
        with_admin_client!(self.auth, AuditServiceClient, |client| {
            Ok(client
                .list_audit_logs(ListAuditLogsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    action: action.map(str::to_string),
                    resource_type: None,
                    resource_id: None,
                    start_created_at: None,
                    end_created_at: None,
                    limit: Some(20),
                    offset: Some(0),
                })
                .await
                .context("ListAuditLogs RPC failed")?
                .into_inner())
        })
    }

    pub async fn get_workspace_settings(&self) -> Result<GetWorkspaceSettingsResponse> {
        with_admin_client!(self.auth, SettingsServiceClient, |client| {
            Ok(client
                .get_workspace_settings(GetWorkspaceSettingsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                })
                .await
                .context("GetWorkspaceSettings RPC failed")?
                .into_inner())
        })
    }

    pub async fn update_workspace_settings(
        &self,
        settings: WorkspaceSettingsInput,
    ) -> Result<UpdateWorkspaceSettingsResponse> {
        with_admin_client!(self.auth, SettingsServiceClient, |client| {
            Ok(client
                .update_workspace_settings(UpdateWorkspaceSettingsRequest {
                    workspace_id: self.auth.workspace_id.clone(),
                    traces_retention_days: settings.traces_retention_days,
                    metrics_retention_days: settings.metrics_retention_days,
                    logs_retention_days: settings.logs_retention_days,
                    max_ingestion_rate: settings.max_ingestion_rate,
                    file_push_interval_secs: settings.file_push_interval_secs,
                    blocked: settings.blocked,
                    capture_llm_content_enabled: settings.capture_llm_content_enabled,
                })
                .await
                .context("UpdateWorkspaceSettings RPC failed")?
                .into_inner())
        })
    }
}
