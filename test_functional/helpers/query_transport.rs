//! Transport-agnostic query client for dual HTTP + gRPC functional tests.

use anyhow::{Context, Result, bail};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::Value;
use std::time::Duration;
use urlencoding::encode;

use crate::TestContext;
use crate::helpers::transport::Transport;
use crate::helpers::{
    ApiClient, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_TIMEOUT, ZradarQueryClient, grpc_not_ready,
    poll_until,
};

/// Filters for span list queries shared across HTTP and gRPC.
#[derive(Debug, Clone, Default)]
pub struct SpanFilters {
    pub trace_id: Option<String>,
    pub operation_name: Option<String>,
}

/// Normalized span row for cross-transport assertions.
#[derive(Debug, Clone)]
pub struct SpanView {
    pub span_id: String,
    pub trace_id: String,
    pub operation_name: String,
    pub duration_ms: i64,
}

/// Normalized trace detail for cross-transport assertions.
#[derive(Debug, Clone)]
pub struct TraceView {
    pub trace_id: String,
    pub spans: Vec<SpanView>,
}

/// Normalized error analytics row.
#[derive(Debug, Clone)]
pub struct ErrorBreakdownView {
    pub error_type: String,
    pub count: i64,
    pub percentage: f64,
}

/// Query API client backed by either HTTP REST or gRPC.
#[derive(Clone)]
pub enum QueryTransportClient {
    Http(ApiClient),
    Grpc(ZradarQueryClient),
}

impl QueryTransportClient {
    pub fn transport(&self) -> Transport {
        match self {
            Self::Http(_) => Transport::Http,
            Self::Grpc(_) => Transport::Grpc,
        }
    }

    pub fn from_test_context(
        ctx: &TestContext,
        transport: Transport,
        api_key: &str,
        workspace_id: &str,
    ) -> Self {
        match transport {
            Transport::Http => {
                let mut client = ApiClient::new(ctx.config.api_url.clone());
                client.set_token(api_key.to_string());
                client.set_workspace_id(workspace_id.to_string());
                Self::Http(client)
            }
            Transport::Grpc => Self::Grpc(
                ZradarQueryClient::new(ctx.config.query_grpc_url.clone())
                    .with_api_key(api_key.to_string())
                    .with_workspace_id(workspace_id.to_string()),
            ),
        }
    }

    /// Poll until a trace has at least one span, then return normalized detail.
    pub async fn wait_for_trace(&self, trace_id_hex: &str) -> Result<TraceView> {
        self.wait_for_trace_timeout(trace_id_hex, DEFAULT_POLL_TIMEOUT)
            .await
    }

    pub async fn wait_for_trace_timeout(
        &self,
        trace_id_hex: &str,
        timeout: Duration,
    ) -> Result<TraceView> {
        let trace_id_hex = trace_id_hex.to_string();
        match self {
            Self::Http(client) => {
                let url = format!("/api/v1/traces/{}", trace_id_hex);
                poll_until(
                    || async {
                        let response = client.get(&url).await?;
                        if !response.status().is_success() {
                            return Ok(None);
                        }
                        let data: Value = response.json().await?;
                        let has_spans = data["spans"]
                            .as_array()
                            .map(|a| !a.is_empty())
                            .unwrap_or(false);
                        if has_spans {
                            Ok(Some(trace_view_from_http(&data, &trace_id_hex)?))
                        } else {
                            Ok(None)
                        }
                    },
                    timeout,
                    DEFAULT_POLL_INTERVAL,
                )
                .await
            }
            Self::Grpc(client) => {
                let client = client.clone();
                poll_until(
                    || async {
                        match client.get_trace(&trace_id_hex).await {
                            Ok(response) => {
                                let trace = response.trace.as_ref();
                                let span_count = trace.map(|t| t.spans.len()).unwrap_or(0);
                                if span_count == 0 {
                                    Ok(None)
                                } else {
                                    Ok(Some(trace_view_from_grpc(trace.unwrap())?))
                                }
                            }
                            Err(err) if grpc_not_ready(&err) => Ok(None),
                            Err(err) => Err(err),
                        }
                    },
                    timeout,
                    DEFAULT_POLL_INTERVAL,
                )
                .await
            }
        }
    }

    /// Poll until span query returns at least one row.
    pub async fn wait_for_spans(&self, filters: &SpanFilters) -> Result<Vec<SpanView>> {
        self.wait_for_spans_timeout(filters, DEFAULT_POLL_TIMEOUT)
            .await
    }

    pub async fn wait_for_spans_timeout(
        &self,
        filters: &SpanFilters,
        timeout: Duration,
    ) -> Result<Vec<SpanView>> {
        let filters = filters.clone();
        match self {
            Self::Http(client) => {
                let url = span_filters_to_http_url(&filters);
                poll_until(
                    || async {
                        let response = client.get(&url).await?;
                        let status = response.status();
                        if !status.is_success() {
                            let body = response.text().await.unwrap_or_default();
                            bail!("wait_for_spans: GET {} returned {}: {}", url, status, body);
                        }
                        let data: Value = response.json().await?;
                        let items = data
                            .get("items")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        if items.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(spans_from_http_items(&items)?))
                        }
                    },
                    timeout,
                    DEFAULT_POLL_INTERVAL,
                )
                .await
            }
            Self::Grpc(client) => {
                let client = client.clone();
                poll_until(
                    || async {
                        let response = client
                            .query_spans_filtered(
                                filters.trace_id.as_deref(),
                                filters.operation_name.as_deref(),
                            )
                            .await?;
                        if response.items.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(spans_from_grpc_items(&response.items)?))
                        }
                    },
                    timeout,
                    DEFAULT_POLL_INTERVAL,
                )
                .await
            }
        }
    }

    /// Query traces over the last hour (time-ranged list).
    pub async fn query_traces_recent(&self) -> Result<Vec<Value>> {
        let (start_rfc, end_rfc) = recent_rfc3339_range();
        match self {
            Self::Http(client) => {
                let path = format!(
                    "/api/v1/traces?start_time={}&end_time={}",
                    encode(&start_rfc),
                    encode(&end_rfc)
                );
                let response = client.get(&path).await?;
                anyhow::ensure!(
                    response.status().is_success(),
                    "Basic trace query should succeed over HTTP"
                );
                let body: Value = response.json().await?;
                Ok(body
                    .get("items")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default())
            }
            Self::Grpc(client) => {
                let response = client.query_traces(None, None).await?;
                Ok(response
                    .items
                    .into_iter()
                    .map(|trace| {
                        serde_json::json!({
                            "trace_id": trace.trace_id,
                            "operation_name": trace.operation_name,
                            "duration_ms": trace.duration_ms,
                        })
                    })
                    .collect())
            }
        }
    }

    /// Error analytics breakdown for the last hour.
    pub async fn get_error_breakdown(&self) -> Result<Vec<ErrorBreakdownView>> {
        let (start_rfc, end_rfc) = recent_rfc3339_range();
        match self {
            Self::Http(client) => {
                let path = format!(
                    "/api/v1/analytics/errors?start_time={}&end_time={}",
                    encode(&start_rfc),
                    encode(&end_rfc)
                );
                let response = client.get(&path).await?;
                let status = response.status();
                if !status.is_success() {
                    let error_text = response.text().await.unwrap_or_default();
                    bail!(
                        "Expected 200 OK for error analytics, got {}: {}",
                        status,
                        error_text
                    );
                }
                let body: Value = response.json().await?;
                let breakdowns = body
                    .as_array()
                    .context("Expected array response for HTTP error analytics")?;
                breakdowns
                    .iter()
                    .map(|row| {
                        Ok(ErrorBreakdownView {
                            error_type: row
                                .get("error_type")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            count: row
                                .get("count")
                                .and_then(|v| v.as_i64())
                                .context("error breakdown missing count")?,
                            percentage: row
                                .get("percentage")
                                .and_then(|v| v.as_f64())
                                .context("error breakdown missing percentage")?,
                        })
                    })
                    .collect()
            }
            Self::Grpc(client) => {
                let response = client.get_error_breakdown().await?;
                Ok(response
                    .errors
                    .into_iter()
                    .map(|row| ErrorBreakdownView {
                        error_type: row.error_type,
                        count: row.count,
                        percentage: row.percentage,
                    })
                    .collect())
            }
        }
    }
}

fn recent_rfc3339_range() -> (String, String) {
    let now = Utc::now();
    (
        (now - ChronoDuration::hours(1)).to_rfc3339(),
        now.to_rfc3339(),
    )
}

fn span_filters_to_http_url(filters: &SpanFilters) -> String {
    let mut url = String::from("/api/v1/spans");
    let mut params = Vec::new();
    if let Some(trace_id) = &filters.trace_id {
        params.push(format!("trace_id={trace_id}"));
    }
    if let Some(operation_name) = &filters.operation_name {
        params.push(format!("operation_name={operation_name}"));
    }
    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }
    url
}

fn trace_view_from_http(data: &Value, trace_id_hex: &str) -> Result<TraceView> {
    let spans = data["spans"]
        .as_array()
        .context("HTTP trace response missing spans array")?;
    Ok(TraceView {
        trace_id: trace_id_hex.to_string(),
        spans: spans_from_http_items(spans)?,
    })
}

fn trace_view_from_grpc(trace: &api::grpc::query_proto::TraceDetail) -> Result<TraceView> {
    Ok(TraceView {
        trace_id: trace.trace_id.clone(),
        spans: spans_from_grpc_items(&trace.spans)?,
    })
}

fn spans_from_http_items(items: &[Value]) -> Result<Vec<SpanView>> {
    items
        .iter()
        .map(|span| {
            Ok(SpanView {
                span_id: span
                    .get("span_id")
                    .and_then(|v| v.as_str())
                    .context("span missing span_id")?
                    .to_string(),
                trace_id: span
                    .get("trace_id")
                    .and_then(|v| v.as_str())
                    .context("span missing trace_id")?
                    .to_string(),
                operation_name: span
                    .get("operation_name")
                    .and_then(|v| v.as_str())
                    .context("span missing operation_name")?
                    .to_string(),
                duration_ms: span
                    .get("duration_ms")
                    .and_then(|v| v.as_i64())
                    .context("span missing duration_ms")?,
            })
        })
        .collect()
}

fn spans_from_grpc_items(items: &[api::grpc::query_proto::SpanDetail]) -> Result<Vec<SpanView>> {
    Ok(items
        .iter()
        .map(|span| SpanView {
            span_id: span.span_id.clone(),
            trace_id: span.trace_id.clone(),
            operation_name: span.operation_name.clone(),
            duration_ms: span.duration_ms,
        })
        .collect())
}
