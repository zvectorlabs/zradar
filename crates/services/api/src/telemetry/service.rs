//! Query service - telemetry use case orchestration

use chrono::DateTime;
use std::{collections::HashMap, sync::Arc, time::Instant};
use uuid::Uuid;

use super::types::{
    AgentAnalytics, AnalyticsQuery, AnalyticsResult, ErrorAnalyticsQuery, ErrorBreakdown,
    IngestRateQuery, LlmAnalytics, LogDetail, LogQueryFilters, MetricDetail, MetricQueryFilters,
    MetricSeriesFilters, MetricSeriesPoint, PaginatedResponse, QueryUsageQuery, QuotaStatusQuery,
    SpanDetail, SpanQueryFilters, StorageUsage, StorageUsageQuery, TopEndpoint, TopNQuery,
    TraceDetail, TraceQueryFilters, TraceSummary, UsageDailyQuery,
};
use crate::errors::{ControlError, Result};

// Use storage-level traits from zradar-traits
use zradar_models::FileListFilter;
use zradar_traits::{
    AnalyticsQueryFilters as StorageAnalyticsFilters, FileListRepository,
    LogQueryFilters as StorageLogFilters, MetricQueryFilters as StorageMetricFilters,
    MetricSeriesFilters as StorageMetricSeriesFilters, Pagination,
    SpanQueryFilters as StorageSpanFilters, TelemetryReader as StorageTelemetryReader, TimeRange,
    TraceQueryFilters as StorageTraceFilters,
};

use zradar_policy::{
    Decision, DecisionSummary, IngestRateRecord, Operation, PolicyEnforcer, PolicyLimit,
    PolicyStore, QueryCtx, QuerySample, QueryUsageRecord, QuotaStatus, SignalKind, ThresholdStatus,
    UsageAnalyticsReader, UsageDailyRecord, UsageReader, UsageTracker,
};
use zradar_retention::QueryEnforcer;

fn non_empty_string(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn signal_type_name(signal: SignalKind) -> Option<&'static str> {
    match signal {
        SignalKind::Traces => Some("traces"),
        SignalKind::Logs => Some("logs"),
        SignalKind::Metrics => Some("metrics"),
        SignalKind::Rum
        | SignalKind::SessionReplay
        | SignalKind::ErrorTracking
        | SignalKind::All => None,
    }
}

fn parse_signal_kind(value: &str) -> Result<SignalKind> {
    match value {
        "traces" => Ok(SignalKind::Traces),
        "logs" => Ok(SignalKind::Logs),
        "metrics" => Ok(SignalKind::Metrics),
        "all" => Ok(SignalKind::All),
        _ => Err(ControlError::InvalidInput(format!(
            "Invalid signal: {value}"
        ))),
    }
}

fn quota_status(
    signal: SignalKind,
    operation: Operation,
    limit_kind: &str,
    limit_value: i64,
    observed_value: i64,
    hard_block_pct: u8,
    period: (Option<i64>, Option<i64>),
) -> QuotaStatus {
    let pct_consumed = if limit_value > 0 {
        observed_value as f64 * 100.0 / limit_value as f64
    } else {
        100.0
    };
    let status = if limit_value <= 0
        || observed_value.saturating_mul(100)
            >= limit_value.saturating_mul(i64::from(hard_block_pct))
    {
        ThresholdStatus::Blocked
    } else if pct_consumed > 100.0 {
        ThresholdStatus::Grace
    } else if pct_consumed >= 90.0 {
        ThresholdStatus::Critical
    } else if pct_consumed >= 70.0 {
        ThresholdStatus::Warning
    } else {
        ThresholdStatus::Ok
    };

    QuotaStatus {
        signal,
        operation,
        limit_kind: limit_kind.to_string(),
        limit_value,
        observed_value,
        pct_consumed,
        status,
        period_start: period.0,
        period_end: period.1,
        projected_exhaustion_at: None,
    }
}

/// Query service for telemetry operations
pub struct QueryService {
    pub storage: Arc<dyn StorageTelemetryReader>,
    pub file_list_repo: Option<Arc<dyn FileListRepository>>,
    /// Optional query enforcer that clamps time ranges to the retention window.
    pub enforcer: Option<Arc<QueryEnforcer>>,
    pub policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    pub usage_tracker: Option<Arc<dyn UsageTracker>>,
    pub policy_store: Option<Arc<dyn PolicyStore>>,
    pub usage_reader: Option<Arc<dyn UsageReader>>,
    pub usage_analytics_reader: Option<Arc<dyn UsageAnalyticsReader>>,
}

impl QueryService {
    /// Create a new QueryService without retention enforcement.
    pub fn new(storage: Arc<dyn StorageTelemetryReader>) -> Self {
        Self {
            storage,
            file_list_repo: None,
            enforcer: None,
            policy_enforcer: None,
            usage_tracker: None,
            policy_store: None,
            usage_reader: None,
            usage_analytics_reader: None,
        }
    }

    /// Create a QueryService with retention enforcement enabled.
    pub fn with_enforcer(
        storage: Arc<dyn StorageTelemetryReader>,
        enforcer: Arc<QueryEnforcer>,
    ) -> Self {
        Self {
            storage,
            file_list_repo: None,
            enforcer: Some(enforcer),
            policy_enforcer: None,
            usage_tracker: None,
            policy_store: None,
            usage_reader: None,
            usage_analytics_reader: None,
        }
    }

    /// Add file metadata access for storage usage analytics.
    pub fn with_file_list_repo(mut self, file_list_repo: Arc<dyn FileListRepository>) -> Self {
        self.file_list_repo = Some(file_list_repo);
        self
    }

    pub fn with_policy_enforcer(mut self, policy_enforcer: Arc<dyn PolicyEnforcer>) -> Self {
        self.policy_enforcer = Some(policy_enforcer);
        self
    }

    pub fn with_usage_tracker(mut self, usage_tracker: Arc<dyn UsageTracker>) -> Self {
        self.usage_tracker = Some(usage_tracker);
        self
    }

    pub fn with_policy_context(
        mut self,
        policy_store: Arc<dyn PolicyStore>,
        usage_reader: Arc<dyn UsageReader>,
        usage_analytics_reader: Arc<dyn UsageAnalyticsReader>,
    ) -> Self {
        self.policy_store = Some(policy_store);
        self.usage_reader = Some(usage_reader);
        self.usage_analytics_reader = Some(usage_analytics_reader);
        self
    }

    /// Apply retention enforcement to a start time (nanoseconds).
    ///
    /// Returns the effective start time after clamping (or the original if no
    /// enforcer is configured, or if the start is within the retention window).
    fn enforce_start(&self, org_id: Uuid, project_id: Uuid, start_ns: Option<i64>) -> Result<i64> {
        if let Some(enforcer) = &self.enforcer {
            let (effective_start, _result) = enforcer
                .enforce(org_id, project_id, start_ns)
                .map_err(|e| ControlError::InvalidInput(e.to_string()))?;
            Ok(effective_start)
        } else {
            Ok(start_ns.unwrap_or(0))
        }
    }

    async fn enforce_policy_query(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> Result<Option<u64>> {
        let estimated_scanned_bytes = self
            .estimate_scanned_bytes(tenant_id, project_id, signal, start_micros, end_micros)
            .await?;
        let Some(enforcer) = &self.policy_enforcer else {
            return Ok(estimated_scanned_bytes);
        };

        match enforcer
            .check_query(QueryCtx {
                tenant_id,
                project_id,
                signal,
                start_micros,
                end_micros,
                estimated_scanned_bytes,
                now_micros: chrono::Utc::now().timestamp_micros(),
            })
            .await
        {
            Decision::Allow | Decision::AllowWithGrace { .. } => Ok(estimated_scanned_bytes),
            Decision::Throttle { reason, .. } | Decision::Block { reason, .. } => {
                Err(ControlError::InvalidInput(reason.to_string()))
            }
        }
    }

    async fn estimate_scanned_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> Result<Option<u64>> {
        let Some(file_list_repo) = &self.file_list_repo else {
            return Ok(None);
        };
        let Some(signal_type) = signal_type_name(signal) else {
            return Ok(None);
        };

        let files = file_list_repo
            .query_files(FileListFilter {
                tenant_id: Some(tenant_id),
                project_id: Some(project_id),
                signal_type: Some(signal_type.to_string()),
                time_range_start: start_micros,
                time_range_end: end_micros,
                deleted: Some(false),
                ..Default::default()
            })
            .await
            .map_err(|e| ControlError::Internal(format!("Storage metadata error: {}", e)))?;

        Ok(Some(files.into_iter().fold(0_u64, |total, file| {
            total.saturating_add(u64::try_from(file.compressed_size).unwrap_or(0))
        })))
    }

    async fn record_query_usage(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        estimated_scanned_bytes: Option<u64>,
        rows_scanned: i64,
        query_started_at: Instant,
    ) {
        let Some(usage_tracker) = &self.usage_tracker else {
            return;
        };

        let usage_tracker = usage_tracker.clone();
        tokio::spawn(async move {
            usage_tracker
                .record_query(QuerySample {
                    tenant_id,
                    project_id,
                    signal,
                    bytes_scanned: i64::try_from(estimated_scanned_bytes.unwrap_or(0))
                        .unwrap_or(i64::MAX),
                    rows_scanned: Some(rows_scanned),
                    query_time_ms: Some(
                        i32::try_from(query_started_at.elapsed().as_millis()).unwrap_or(i32::MAX),
                    ),
                    decision: DecisionSummary::Allow,
                    submitted_at: chrono::Utc::now().timestamp_micros(),
                })
                .await;
        });
    }

    /// Query traces
    pub async fn query_traces(
        &self,
        tenant_id: Uuid,
        filters: TraceQueryFilters,
    ) -> Result<PaginatedResponse<TraceSummary>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let start_micros = filters.start_time.as_ref().map(|t| t.timestamp_micros());
        let end_micros = filters.end_time.as_ref().map(|t| t.timestamp_micros());
        let estimated_scanned_bytes = self
            .enforce_policy_query(
                tenant_id,
                project_id,
                SignalKind::Traces,
                start_micros,
                end_micros,
            )
            .await?;
        let query_started_at = Instant::now();

        let raw_start = filters
            .start_time
            .as_ref()
            .and_then(|t| t.timestamp_nanos_opt());
        let enforced_start = self.enforce_start(tenant_id, project_id, raw_start)?;

        // Convert API filters to storage filters
        let storage_filters = StorageTraceFilters {
            project_id: Some(project_id),
            time_range: filters.end_time.map(|end| TimeRange {
                start: enforced_start,
                end: end.timestamp_nanos_opt().unwrap_or(0),
            }),
            service_name: filters.service_name.clone(),
            status: filters.status.clone(),
            min_duration_ms: filters.min_duration_ms.map(|d| d as u64),
            max_duration_ms: filters.max_duration_ms.map(|d| d as u64),
            llm_model: filters.llm_model.clone(),
            llm_provider: filters.llm_provider.clone(),
            agent_name: filters.agent_name.clone(),
            session_id: filters.session_id.clone(),
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
        let rows_scanned = i64::try_from(result.items.len()).unwrap_or(i64::MAX);
        self.record_query_usage(
            tenant_id,
            project_id,
            SignalKind::Traces,
            estimated_scanned_bytes,
            rows_scanned,
            query_started_at,
        )
        .await;

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
        tenant_id: Uuid,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<TraceDetail> {
        // Query storage for spans in this trace
        let spans = self
            .storage
            .get_trace_detail(tenant_id, project_id, trace_id)
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
                agent_name: non_empty_string(s.agent_name.clone()),
                agent_type: non_empty_string(s.agent_type.clone()),
                session_id: non_empty_string(s.session_id.clone()),
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
        tenant_id: Uuid,
        filters: SpanQueryFilters,
    ) -> Result<PaginatedResponse<SpanDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let start_micros = filters.start_time.as_ref().map(|t| t.timestamp_micros());
        let end_micros = filters.end_time.as_ref().map(|t| t.timestamp_micros());
        let estimated_scanned_bytes = self
            .enforce_policy_query(
                tenant_id,
                project_id,
                SignalKind::Traces,
                start_micros,
                end_micros,
            )
            .await?;
        let query_started_at = Instant::now();

        let raw_start = filters
            .start_time
            .as_ref()
            .and_then(|t| t.timestamp_nanos_opt());
        let enforced_start = self.enforce_start(tenant_id, project_id, raw_start)?;

        // Parse and validate span_types
        let span_types = filters
            .parse_span_types()
            .map_err(ControlError::InvalidInput)?;

        // Convert API filters to storage filters
        let storage_filters = StorageSpanFilters {
            project_id: Some(project_id),
            trace_id: filters.trace_id.clone(),
            time_range: filters.end_time.map(|end| TimeRange {
                start: enforced_start,
                end: end.timestamp_nanos_opt().unwrap_or(0),
            }),
            service_name: filters.service_name.clone(),
            span_name: filters.operation_name.clone(),
            span_types: span_types.clone(),
            status: None,
            llm_model: filters.llm_model.clone(),
            agent_name: filters.agent_name.clone(),
            session_id: filters.session_id.clone(),
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
        let rows_scanned = i64::try_from(result.items.len()).unwrap_or(i64::MAX);
        self.record_query_usage(
            tenant_id,
            project_id,
            SignalKind::Traces,
            estimated_scanned_bytes,
            rows_scanned,
            query_started_at,
        )
        .await;

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
                agent_name: non_empty_string(s.agent_name),
                agent_type: non_empty_string(s.agent_type),
                session_id: non_empty_string(s.session_id),
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
        tenant_id: Uuid,
        project_id: Uuid,
        span_id: &str,
    ) -> Result<SpanDetail> {
        let span = self
            .storage
            .get_span(tenant_id, project_id, span_id)
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
            agent_name: non_empty_string(span.agent_name),
            agent_type: non_empty_string(span.agent_type),
            session_id: non_empty_string(span.session_id),
            attributes: serde_json::from_str(&span.attributes).unwrap_or_default(),
        })
    }

    /// Get analytics — unified grouped time-series endpoint.
    ///
    /// When `metric` or `group_by` is supplied, delegates to the generic
    /// `query_analytics()` storage method. Otherwise falls back to the
    /// original `get_daily_trace_counts()` for backward compatibility.
    pub async fn get_analytics(
        &self,
        tenant_id: Uuid,
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

        // Parse comma-separated group_by string into Vec
        let group_by_vec: Vec<String> = query
            .group_by
            .as_ref()
            .map(|s| s.split(',').map(|item| item.trim().to_string()).collect())
            .unwrap_or_default();

        let has_group_by = !group_by_vec.is_empty();
        let has_metric = query.metric.as_ref().is_some_and(|m| m != "trace_count");
        let has_filters = query.filters.as_ref().is_some_and(|f| !f.is_empty());

        // Use the generic analytics path when the caller requests grouping,
        // a non-default metric, or ad-hoc filters.
        if has_group_by || has_metric || has_filters {
            let storage_filters = StorageAnalyticsFilters {
                project_id,
                start,
                end,
                metric: query.metric.unwrap_or_else(|| "trace_count".to_string()),
                group_by: group_by_vec,
                filters: query.filters.unwrap_or_default(),
            };

            let points = self
                .storage
                .query_analytics(tenant_id, storage_filters)
                .await
                .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

            let results = points
                .into_iter()
                .map(|p| {
                    let ts_secs = p.bucket_ts / 1_000_000_000;
                    let dt = chrono::DateTime::from_timestamp(ts_secs, 0)
                        .unwrap_or_default()
                        .to_rfc3339();
                    AnalyticsResult {
                        timestamp: dt,
                        value: p.value,
                        groups: if p.groups.is_empty() {
                            None
                        } else {
                            Some(p.groups)
                        },
                    }
                })
                .collect();

            return Ok(results);
        }

        // Backward-compatible default: daily trace counts without grouping.
        let points = self
            .storage
            .get_daily_trace_counts(tenant_id, project_id, start, end)
            .await
            .map_err(|e| ControlError::Internal(format!("Storage error: {}", e)))?;

        let results = points
            .into_iter()
            .map(|p| AnalyticsResult {
                timestamp: p.timestamp,
                value: p.value,
                ..Default::default()
            })
            .collect();

        Ok(results)
    }

    /// Get metrics summary
    pub async fn get_metrics_summary(
        &self,
        tenant_id: Uuid,
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
            .get_metrics_summary(tenant_id, project_id, start, end)
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
        tenant_id: Uuid,
        query: TopNQuery,
    ) -> Result<Vec<TopEndpoint>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let limit = query.n.unwrap_or(10).max(1) as usize;
        let start = query.start_time.timestamp_nanos_opt().unwrap_or(0);
        let end = query.end_time.timestamp_nanos_opt().unwrap_or(0);

        let points = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "span_count".to_string(),
                    group_by: vec!["service_name".to_string(), "span_name".to_string()],
                    filters: HashMap::new(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        let durations = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "avg_duration_ms".to_string(),
                    group_by: vec!["service_name".to_string(), "span_name".to_string()],
                    filters: HashMap::new(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        let errors = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "error_count".to_string(),
                    group_by: vec!["service_name".to_string(), "span_name".to_string()],
                    filters: HashMap::new(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        let mut endpoints: Vec<TopEndpoint> = points
            .into_iter()
            .map(|point| {
                let operation_name = point.groups.get("span_name").cloned().unwrap_or_default();
                let service_name = point
                    .groups
                    .get("service_name")
                    .cloned()
                    .unwrap_or_default();
                let error_count =
                    find_endpoint_group_value(&errors, &service_name, &operation_name);
                TopEndpoint {
                    operation_name: operation_name.clone(),
                    service_name: service_name.clone(),
                    count: point.value as i64,
                    avg_duration_ms: find_endpoint_group_value(
                        &durations,
                        &service_name,
                        &operation_name,
                    ),
                    p95_duration_ms: None,
                    error_rate: if point.value > 0.0 {
                        error_count / point.value
                    } else {
                        0.0
                    },
                }
            })
            .collect();

        endpoints.sort_by(|a, b| b.count.cmp(&a.count));
        endpoints.truncate(limit);
        Ok(endpoints)
    }

    /// Get error breakdown
    pub async fn get_error_breakdown(
        &self,
        tenant_id: Uuid,
        query: ErrorAnalyticsQuery,
    ) -> Result<Vec<ErrorBreakdown>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let mut filters = HashMap::new();
        filters.insert("status_code".to_string(), "ERROR".to_string());
        if let Some(service_name) = query.service_name {
            filters.insert("service_name".to_string(), service_name);
        }
        let points = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start: query.start_time.timestamp_nanos_opt().unwrap_or(0),
                    end: query.end_time.timestamp_nanos_opt().unwrap_or(0),
                    metric: "error_count".to_string(),
                    group_by: vec!["service_name".to_string()],
                    filters,
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        let total: f64 = points.iter().map(|p| p.value).sum();
        Ok(points
            .into_iter()
            .map(|point| ErrorBreakdown {
                error_type: point
                    .groups
                    .get("service_name")
                    .cloned()
                    .unwrap_or_default(),
                count: point.value as i64,
                percentage: if total > 0.0 {
                    point.value / total * 100.0
                } else {
                    0.0
                },
            })
            .collect())
    }

    pub async fn get_llm_analytics(
        &self,
        tenant_id: Uuid,
        query: AnalyticsQuery,
    ) -> Result<Vec<LlmAnalytics>> {
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
        let filters = HashMap::new();

        let requests = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "span_count".to_string(),
                    group_by: vec!["llm_model".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let tokens = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "total_tokens".to_string(),
                    group_by: vec!["llm_model".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let costs = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "total_cost_usd".to_string(),
                    group_by: vec!["llm_model".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let durations = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "avg_duration_ms".to_string(),
                    group_by: vec!["llm_model".to_string()],
                    filters,
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        Ok(requests
            .into_iter()
            .map(|point| {
                let llm_model = point.groups.get("llm_model").cloned().unwrap_or_default();
                LlmAnalytics {
                    request_count: point.value as i64,
                    total_tokens: find_group_value(&tokens, "llm_model", &llm_model),
                    total_cost_usd: find_group_value(&costs, "llm_model", &llm_model),
                    avg_duration_ms: find_group_value(&durations, "llm_model", &llm_model),
                    llm_model,
                }
            })
            .collect())
    }

    pub async fn get_agent_analytics(
        &self,
        tenant_id: Uuid,
        query: AnalyticsQuery,
    ) -> Result<Vec<AgentAnalytics>> {
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
        let filters = HashMap::new();

        let spans = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "span_count".to_string(),
                    group_by: vec!["agent_name".to_string(), "agent_type".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let errors = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "error_count".to_string(),
                    group_by: vec!["agent_name".to_string(), "agent_type".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let tokens = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "total_tokens".to_string(),
                    group_by: vec!["agent_name".to_string(), "agent_type".to_string()],
                    filters: filters.clone(),
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;
        let durations = self
            .storage
            .query_analytics(
                tenant_id,
                StorageAnalyticsFilters {
                    project_id,
                    start,
                    end,
                    metric: "avg_duration_ms".to_string(),
                    group_by: vec!["agent_name".to_string(), "agent_type".to_string()],
                    filters,
                },
            )
            .await
            .map_err(|e| ControlError::Internal(format!("Analytics error: {}", e)))?;

        Ok(spans
            .into_iter()
            .map(|point| {
                let agent_name = point.groups.get("agent_name").cloned().unwrap_or_default();
                let agent_type = point.groups.get("agent_type").cloned();
                AgentAnalytics {
                    span_count: point.value as i64,
                    error_count: find_agent_group_value(&errors, &agent_name, agent_type.as_deref())
                        as i64,
                    total_tokens: find_agent_group_value(
                        &tokens,
                        &agent_name,
                        agent_type.as_deref(),
                    ),
                    avg_duration_ms: find_agent_group_value(
                        &durations,
                        &agent_name,
                        agent_type.as_deref(),
                    ),
                    agent_name,
                    agent_type,
                }
            })
            .collect())
    }

    /// Return active storage usage grouped by signal type and storage location.
    pub async fn get_storage_usage(
        &self,
        tenant_id: Uuid,
        filters: StorageUsageQuery,
    ) -> Result<Vec<StorageUsage>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let file_list_repo = self.file_list_repo.as_ref().ok_or_else(|| {
            ControlError::Internal(
                "Storage usage metadata repository is not configured".to_string(),
            )
        })?;

        let files = file_list_repo
            .query_files(FileListFilter {
                tenant_id: Some(tenant_id),
                project_id: Some(project_id),
                signal_type: filters.signal_type,
                time_range_start: filters.start_time.map(|start| start.timestamp_micros()),
                time_range_end: filters.end_time.map(|end| end.timestamp_micros()),
                location: filters.location,
                deleted: Some(false),
                ..Default::default()
            })
            .await
            .map_err(|e| ControlError::Internal(format!("Storage metadata error: {}", e)))?;

        let mut grouped: HashMap<(String, String), StorageUsage> = HashMap::new();
        for file in files {
            let key = (file.signal_type.clone(), file.location.clone());
            let usage = grouped.entry(key).or_insert_with(|| StorageUsage {
                tenant_id: file.tenant_id.to_string(),
                project_id: file.project_id.to_string(),
                signal_type: file.signal_type.clone(),
                location: file.location.clone(),
                file_count: 0,
                records: 0,
                original_size: 0,
                compressed_size: 0,
            });
            usage.file_count += 1;
            usage.records += file.records;
            usage.original_size += file.original_size;
            usage.compressed_size += file.compressed_size;
        }

        let mut usage: Vec<StorageUsage> = grouped.into_values().collect();
        usage.sort_by(|a, b| {
            a.signal_type
                .cmp(&b.signal_type)
                .then_with(|| a.location.cmp(&b.location))
        });
        Ok(usage)
    }

    pub async fn get_quota_status(
        &self,
        tenant_id: Uuid,
        query: QuotaStatusQuery,
    ) -> Result<Vec<QuotaStatus>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let policy_store = self
            .policy_store
            .as_ref()
            .ok_or_else(|| ControlError::Internal("Policy store is not configured".to_string()))?;
        let usage_reader = self
            .usage_reader
            .as_ref()
            .ok_or_else(|| ControlError::Internal("Usage reader is not configured".to_string()))?;

        let signals = match query.signal.as_deref() {
            Some(signal) => vec![parse_signal_kind(signal)?],
            None => vec![SignalKind::Traces, SignalKind::Logs, SignalKind::Metrics],
        };
        let mut statuses = Vec::new();

        for signal in signals {
            for operation in [Operation::Ingest, Operation::Query] {
                let resolved = policy_store.resolve(tenant_id, project_id, signal, operation);
                for quota in resolved.quotas {
                    if let PolicyLimit::Quota {
                        max_bytes,
                        period_start,
                        period_end,
                        ..
                    } = quota
                    {
                        let observed_value = usage_reader
                            .period_used_bytes(
                                tenant_id,
                                project_id,
                                signal,
                                operation,
                                period_start,
                                period_end,
                            )
                            .await
                            .map_err(|e| ControlError::Internal(e.to_string()))?;
                        statuses.push(quota_status(
                            signal,
                            operation,
                            "quota",
                            max_bytes,
                            observed_value,
                            resolved.hard_block_pct,
                            (Some(period_start), period_end),
                        ));
                    }
                }
            }

            let resolved = policy_store.resolve(tenant_id, project_id, signal, Operation::Store);
            if let Some(PolicyLimit::Size { max_bytes, .. }) = resolved.size {
                let observed_value = usage_reader
                    .stored_compressed_bytes(tenant_id, project_id, signal)
                    .await
                    .map_err(|e| ControlError::Internal(e.to_string()))?;
                statuses.push(quota_status(
                    signal,
                    Operation::Store,
                    "size",
                    max_bytes,
                    observed_value,
                    resolved.hard_block_pct,
                    (None, None),
                ));
            }
        }

        Ok(statuses)
    }

    pub async fn get_usage_daily(
        &self,
        tenant_id: Uuid,
        query: UsageDailyQuery,
    ) -> Result<Vec<UsageDailyRecord>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let usage_analytics_reader = self.usage_analytics_reader.as_ref().ok_or_else(|| {
            ControlError::Internal("Usage analytics reader is not configured".to_string())
        })?;
        let signal = query.signal.as_deref().map(parse_signal_kind).transpose()?;

        usage_analytics_reader
            .usage_daily(
                tenant_id,
                project_id,
                signal,
                query.start_time.map(|start| start.timestamp_micros()),
                query.end_time.map(|end| end.timestamp_micros()),
            )
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))
    }

    pub async fn get_ingest_rate(
        &self,
        tenant_id: Uuid,
        query: IngestRateQuery,
    ) -> Result<Vec<IngestRateRecord>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let usage_analytics_reader = self.usage_analytics_reader.as_ref().ok_or_else(|| {
            ControlError::Internal("Usage analytics reader is not configured".to_string())
        })?;
        let signal = query.signal.as_deref().map(parse_signal_kind).transpose()?;
        let now = chrono::Utc::now();
        let end = query.end_time.unwrap_or(now).timestamp_micros();
        let start = query
            .start_time
            .unwrap_or(now - chrono::Duration::seconds(60))
            .timestamp_micros();

        usage_analytics_reader
            .ingest_rate(tenant_id, project_id, signal, start, end)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))
    }

    pub async fn get_query_usage(
        &self,
        tenant_id: Uuid,
        query: QueryUsageQuery,
    ) -> Result<Vec<QueryUsageRecord>> {
        let project_id = Uuid::parse_str(&query.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let usage_analytics_reader = self.usage_analytics_reader.as_ref().ok_or_else(|| {
            ControlError::Internal("Usage analytics reader is not configured".to_string())
        })?;
        let signal = query.signal.as_deref().map(parse_signal_kind).transpose()?;
        let now = chrono::Utc::now();
        let end = query.end_time.unwrap_or(now).timestamp_micros();
        let start = query
            .start_time
            .unwrap_or(now - chrono::Duration::hours(1))
            .timestamp_micros();

        usage_analytics_reader
            .query_usage(tenant_id, project_id, signal, start, end)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))
    }

    /// Query log records
    pub async fn query_logs(
        &self,
        tenant_id: Uuid,
        filters: LogQueryFilters,
    ) -> Result<PaginatedResponse<LogDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let start_micros = filters.start_time.as_ref().map(|t| t.timestamp_micros());
        let end_micros = filters.end_time.as_ref().map(|t| t.timestamp_micros());
        let estimated_scanned_bytes = self
            .enforce_policy_query(
                tenant_id,
                project_id,
                SignalKind::Logs,
                start_micros,
                end_micros,
            )
            .await?;

        let query_started_at = Instant::now();

        let raw_start = filters
            .start_time
            .as_ref()
            .and_then(|t| t.timestamp_nanos_opt());
        let enforced_start = self.enforce_start(tenant_id, project_id, raw_start)?;

        let storage_filters = StorageLogFilters {
            project_id: Some(project_id),
            time_range: filters.end_time.map(|end| TimeRange {
                start: enforced_start,
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
        let rows_scanned = i64::try_from(result.items.len()).unwrap_or(i64::MAX);
        self.record_query_usage(
            tenant_id,
            project_id,
            SignalKind::Logs,
            estimated_scanned_bytes,
            rows_scanned,
            query_started_at,
        )
        .await;

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
        tenant_id: Uuid,
        project_id: Uuid,
        log_id: &str,
    ) -> Result<LogDetail> {
        let log = self
            .storage
            .get_log(tenant_id, project_id, log_id)
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
        tenant_id: Uuid,
        filters: MetricQueryFilters,
    ) -> Result<PaginatedResponse<MetricDetail>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;

        let start_micros = filters.start_time.as_ref().map(|t| t.timestamp_micros());
        let end_micros = filters.end_time.as_ref().map(|t| t.timestamp_micros());
        let estimated_scanned_bytes = self
            .enforce_policy_query(
                tenant_id,
                project_id,
                SignalKind::Metrics,
                start_micros,
                end_micros,
            )
            .await?;

        let query_started_at = Instant::now();

        let raw_start = filters
            .start_time
            .as_ref()
            .and_then(|t| t.timestamp_nanos_opt());
        let enforced_start = self.enforce_start(tenant_id, project_id, raw_start)?;

        let storage_filters = StorageMetricFilters {
            project_id: Some(project_id),
            time_range: filters.end_time.map(|end| TimeRange {
                start: enforced_start,
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
        let rows_scanned = i64::try_from(result.items.len()).unwrap_or(i64::MAX);
        self.record_query_usage(
            tenant_id,
            project_id,
            SignalKind::Metrics,
            estimated_scanned_bytes,
            rows_scanned,
            query_started_at,
        )
        .await;

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
        tenant_id: Uuid,
        filters: MetricSeriesFilters,
    ) -> Result<Vec<MetricSeriesPoint>> {
        let project_id = Uuid::parse_str(&filters.project_id)
            .map_err(|_| ControlError::InvalidInput("Invalid project ID".to_string()))?;
        let start_micros = filters.start_time.as_ref().map(|t| t.timestamp_micros());
        let end_micros = filters.end_time.as_ref().map(|t| t.timestamp_micros());
        let estimated_scanned_bytes = self
            .enforce_policy_query(
                tenant_id,
                project_id,
                SignalKind::Metrics,
                start_micros,
                end_micros,
            )
            .await?;
        let query_started_at = Instant::now();

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
        let rows_scanned = i64::try_from(points.len()).unwrap_or(i64::MAX);
        self.record_query_usage(
            tenant_id,
            project_id,
            SignalKind::Metrics,
            estimated_scanned_bytes,
            rows_scanned,
            query_started_at,
        )
        .await;

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

fn find_group_value(points: &[zradar_traits::AnalyticsDataPoint], key: &str, value: &str) -> f64 {
    points
        .iter()
        .find(|point| {
            point
                .groups
                .get(key)
                .is_some_and(|group_value| group_value == value)
        })
        .map(|point| point.value)
        .unwrap_or(0.0)
}

fn find_endpoint_group_value(
    points: &[zradar_traits::AnalyticsDataPoint],
    service_name: &str,
    operation_name: &str,
) -> f64 {
    points
        .iter()
        .find(|point| {
            point
                .groups
                .get("service_name")
                .is_some_and(|value| value == service_name)
                && point
                    .groups
                    .get("span_name")
                    .is_some_and(|value| value == operation_name)
        })
        .map(|point| point.value)
        .unwrap_or(0.0)
}

fn find_agent_group_value(
    points: &[zradar_traits::AnalyticsDataPoint],
    agent_name: &str,
    agent_type: Option<&str>,
) -> f64 {
    points
        .iter()
        .find(|point| {
            point
                .groups
                .get("agent_name")
                .is_some_and(|value| value == agent_name)
                && point
                    .groups
                    .get("agent_type")
                    .map(|value| Some(value.as_str()) == agent_type)
                    .unwrap_or(agent_type.is_none())
        })
        .map(|point| point.value)
        .unwrap_or(0.0)
}
