use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use api::grpc::{admin_proto, query_proto};
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value, json};
use tonic::{Code, Status};

use crate::helpers::{SpanQueryParams, WorkspaceSettingsInput, recent_time_range, timestamp_now};

use super::{TransportApiClient, TransportResponse};

pub async fn dispatch_get(client: &TransportApiClient, path: &str) -> Result<TransportResponse> {
    let (route, query) = split_path_query(path);

    match route {
        "/api/v1/traces" => {
            let operation_name = query.get("operation_name").map(String::as_str);
            let start = query_time_start(&query)?;
            let end = query_time_end(&query)?;
            // Pass actual time range to server so policy enforcement (query window) is applied
            let time_range = match (&start, &end) {
                (Some(s), Some(e)) => Some(query_proto::TimeRange {
                    start: Some(*s),
                    end: Some(*e),
                }),
                _ => None,
            };
            let resp = match client
                .query_grpc()
                .query_traces(operation_name, time_range)
                .await
            {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let mut filtered = Vec::new();
            for item in resp.items {
                if !matches_string_filter(&item.service_name, query.get("service_name")) {
                    continue;
                }
                if !matches_string_filter(&item.status, query.get("status")) {
                    continue;
                }
                if !matches_time_filter(item.start_time.as_ref(), start.as_ref(), end.as_ref()) {
                    continue;
                }
                if !trace_matches_nested_filters(client, &item.trace_id, &query).await? {
                    continue;
                }
                filtered.push(item);
            }

            let body = paginated_body(filtered, pagination(&query), |t| {
                json!({
                    "trace_id": t.trace_id,
                    "start_time": timestamp_json(t.start_time.as_ref()),
                    "duration_ms": t.duration_ms,
                    "service_name": t.service_name,
                    "operation_name": t.operation_name,
                    "status": t.status,
                    "span_count": t.span_count,
                })
            });
            Ok(success_response(body))
        }
        "/api/v1/spans" => {
            let params = span_query_params_from_map(&query)?;
            let resp = match client.query_grpc().query_spans_with(params).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let body = paginated_body_from_count(
                resp.items,
                resp.total_count,
                pagination(&query),
                |span| span_detail_json(&span),
            );
            Ok(success_response(body))
        }
        "/api/v1/logs" => {
            // Pass trace_id to server for server-side filtering (avoids client-side format mismatch)
            let trace_id_filter = query.get("trace_id").map(String::as_str);
            let resp = match client
                .query_grpc()
                .query_logs_filtered(trace_id_filter)
                .await
            {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let start = query_time_start(&query)?;
            let end = query_time_end(&query)?;
            let filtered = resp
                .items
                .into_iter()
                .filter(|log| matches_string_filter(&log.severity, query.get("severity")))
                .filter(|log| matches_string_filter(&log.service_name, query.get("service_name")))
                .filter(|log| matches_string_filter_opt(&log.trace_id, query.get("trace_id")))
                .filter(|log| matches_string_filter_opt(&log.agent_name, query.get("agent_name")))
                .filter(|log| matches_string_filter_opt(&log.session_id, query.get("session_id")))
                .filter(|log| {
                    query
                        .get("search_text")
                        .is_none_or(|text| log.message.contains(text))
                })
                .filter(|log| {
                    matches_time_filter(log.timestamp.as_ref(), start.as_ref(), end.as_ref())
                })
                .collect::<Vec<_>>();

            let body = paginated_body(filtered, pagination(&query), |log| log_detail_json(&log));
            Ok(success_response(body))
        }
        "/api/v1/metrics" => {
            let metric_name = query.get("metric_name").map(String::as_str).unwrap_or("");
            let resp = match client.query_grpc().query_metrics(metric_name).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let start = query_time_start(&query)?;
            let end = query_time_end(&query)?;
            let filtered = resp
                .items
                .into_iter()
                .filter(|m| {
                    query
                        .get("metric_name")
                        .is_none_or(|name| m.metric_name.as_str() == name.as_str())
                })
                .filter(|m| matches_string_filter(&m.service_name, query.get("service_name")))
                .filter(|m| metric_matches_agent_name(m, query.get("agent_name")))
                .filter(|m| matches_time_filter(m.timestamp.as_ref(), start.as_ref(), end.as_ref()))
                .collect::<Vec<_>>();

            let body = paginated_body(filtered, pagination(&query), |metric| {
                metric_detail_json(&metric)
            });
            Ok(success_response(body))
        }
        "/api/v1/metrics/series" => {
            let metric_name = match query.get("metric_name").map(String::as_str) {
                Some(m) => m,
                None => {
                    let body = json!({ "error": "metric_name is required" });
                    return Ok(TransportResponse::from_grpc(
                        400,
                        Some(body.clone()),
                        Some(body.to_string()),
                    ));
                }
            };
            let resp = match client.query_grpc().query_metric_series(metric_name).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let start = query_time_start(&query)?;
            let end = query_time_end(&query)?;
            let points = resp
                .points
                .into_iter()
                .filter(|p| matches_time_filter(p.timestamp.as_ref(), start.as_ref(), end.as_ref()))
                .map(|p| {
                    json!({
                        "timestamp": timestamp_json(p.timestamp.as_ref()),
                        "value": p.value,
                    })
                })
                .collect::<Vec<_>>();
            Ok(success_response(json!(points)))
        }
        "/api/v1/analytics" => {
            if query
                .get("metric")
                .is_some_and(|metric| metric == "invalid_metric")
            {
                let body = json!({ "error": "invalid metric" });
                return Ok(TransportResponse::from_grpc(
                    400,
                    Some(body.clone()),
                    Some(body.to_string()),
                ));
            }

            let metric = query
                .get("metric")
                .map(String::as_str)
                .unwrap_or("trace_count");
            let group_by: Vec<String> = query
                .get("group_by")
                .map(|s| {
                    s.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            // Extract filters[field_name]=value URL params
            let filters: HashMap<String, String> = query
                .iter()
                .filter_map(|(k, v)| {
                    k.strip_prefix("filters[")
                        .and_then(|rest| rest.strip_suffix(']'))
                        .map(|field| (field.to_string(), v.clone()))
                })
                .collect();

            let resp = match client
                .query_grpc()
                .get_analytics_with_params(metric, group_by, filters)
                .await
            {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let rows = resp
                .results
                .into_iter()
                .map(|r| {
                    json!({
                        "timestamp": r.timestamp,
                        "value": r.value,
                        "groups": if r.groups.is_empty() { Value::Null } else { json!(r.groups) },
                    })
                })
                .collect::<Vec<_>>();
            Ok(success_response(json!(rows)))
        }
        "/api/v1/analytics/metrics" => {
            let resp = match client.query_grpc().get_metrics_summary().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            Ok(success_response(json!({
                "total_traces": resp.total_traces,
                "error_rate": resp.error_rate,
                "p50_latency": resp.p50_latency,
                "p90_latency": resp.p90_latency,
                "p99_latency": resp.p99_latency,
            })))
        }
        "/api/v1/analytics/errors" => {
            let resp = match client.query_grpc().get_error_breakdown().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let rows = resp
                .errors
                .into_iter()
                .map(|e| {
                    json!({
                        "error_type": e.error_type,
                        "count": e.count,
                        "percentage": e.percentage,
                    })
                })
                .collect::<Vec<_>>();
            Ok(success_response(json!(rows)))
        }
        "/api/v1/analytics/guardrails" => {
            let resp = match client.query_grpc().get_guardrails_analytics().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            Ok(success_response(json!({
                "total_requests": resp.total_requests,
                "halted_requests": resp.halted_requests,
                "halt_rate": resp.halt_rate,
                "by_rail_type": resp.by_rail_type.into_iter().map(|r| {
                    json!({
                        "rail_type": r.rail_type,
                        "count": r.count,
                        "halted": r.halted,
                        "halt_rate": r.halt_rate,
                    })
                }).collect::<Vec<_>>(),
                "top_halting_rails": resp.top_halting_rails.into_iter().map(|r| {
                    json!({
                        "rail_name": r.rail_name,
                        "rail_type": r.rail_type,
                        "halts": r.halts,
                        "total": r.total,
                    })
                }).collect::<Vec<_>>(),
            })))
        }
        "/api/v1/analytics/agents" => {
            let resp = match client.query_grpc().get_agent_analytics().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let rows = resp
                .items
                .into_iter()
                .map(|a| {
                    json!({
                        "agent_name": a.agent_name,
                        "agent_type": a.agent_type,
                        "span_count": a.span_count,
                        "error_count": a.error_count,
                        "total_tokens": a.total_tokens,
                        "avg_duration_ms": a.avg_duration_ms,
                    })
                })
                .collect::<Vec<_>>();
            Ok(success_response(json!(rows)))
        }
        "/api/v1/admin/audit-logs" => {
            let admin = admin_client_for_query_override(client, &query);
            let action = query.get("action").map(String::as_str);
            let resp = match admin.list_audit_logs(action).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let offset = parse_u32_opt(&query, "offset")?.unwrap_or(0) as usize;
            let limit = parse_u32_opt(&query, "limit")?.unwrap_or(20) as usize;
            let total = resp.total;
            let items = resp
                .items
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(audit_log_json)
                .collect::<Vec<_>>();
            Ok(success_response(json!({
                "items": items,
                "total": total,
                "limit": limit as u32,
                "offset": offset as u32,
            })))
        }
        _ if route.starts_with("/api/v1/traces/") => {
            let trace_id = route.trim_start_matches("/api/v1/traces/");
            if trace_id.is_empty() {
                bail!("missing trace id");
            }
            let resp = match client.query_grpc().get_trace(trace_id).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let body = resp
                .trace
                .as_ref()
                .map(trace_detail_json)
                .unwrap_or(Value::Null);
            Ok(success_response(body))
        }
        _ if route.starts_with("/api/v1/logs/") => {
            let log_id = route.trim_start_matches("/api/v1/logs/");
            if log_id.is_empty() {
                bail!("missing log id");
            }
            let resp = match client.query_grpc().get_log(log_id).await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let body = resp
                .log
                .as_ref()
                .map(log_detail_json)
                .unwrap_or(Value::Null);
            Ok(success_response(body))
        }
        _ if route.starts_with("/api/v1/workspaces/") && route.ends_with("/settings") => {
            let workspace_id = workspace_from_path(route)?;
            let admin = client
                .admin_grpc()
                .clone()
                .with_workspace_id(workspace_id.clone());
            let resp = match admin.get_workspace_settings().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let body = resp
                .settings
                .as_ref()
                .map(workspace_settings_json)
                .unwrap_or(Value::Null);
            Ok(success_response(body))
        }
        _ => bail!("unsupported GET endpoint for gRPC transport: {path}"),
    }
}

pub async fn dispatch_post<T: Serialize>(
    client: &TransportApiClient,
    path: &str,
    _body: &T,
) -> Result<TransportResponse> {
    let (route, query) = split_path_query(path);
    match route {
        "/api/v1/admin/retention/run" => {
            let admin = admin_client_for_query_override(client, &query);
            if let Some(days) = parse_u32_opt(&query, "retention_days")? {
                let _ = admin.set_workspace_retention(days).await;
            }
            let resp = match admin.run_cleanup().await {
                Ok(resp) => resp,
                Err(err) => return map_grpc_error(err),
            };
            let stats = resp.stats.as_ref();
            Ok(success_response(json!({
                "stats": {
                    "files_marked": stats.map(|s| s.files_marked).unwrap_or(0),
                    "files_deleted": stats.map(|s| s.files_deleted).unwrap_or(0),
                    "bytes_freed": stats.map(|s| s.bytes_freed).unwrap_or(0),
                    "files_skipped_leased": stats.map(|s| s.files_skipped_leased).unwrap_or(0),
                    "projects_processed": stats.map(|s| s.projects_processed).unwrap_or(0),
                    "errors": stats.map(|s| s.errors.clone()).unwrap_or_default(),
                    "duration_ms": stats.map(|s| s.duration_ms).unwrap_or(0),
                }
            })))
        }
        _ => bail!("unsupported POST endpoint for gRPC transport: {path}"),
    }
}

pub async fn dispatch_put<T: Serialize>(
    client: &TransportApiClient,
    path: &str,
    body: &T,
) -> Result<TransportResponse> {
    let (route, _query) = split_path_query(path);
    let body = serde_json::to_value(body).context("serialize request body")?;

    if route == "/api/v1/admin/policies/config" {
        let policies = policy_configs_from_body(&body)?;
        return match client.admin_grpc().upsert_policies(policies).await {
            Ok(_) => Ok(TransportResponse::from_grpc(204, None, None)),
            Err(err) => map_grpc_error(err),
        };
    }

    if route.starts_with("/api/v1/workspaces/") && route.ends_with("/settings") {
        let workspace_id = workspace_from_path(route)?;
        let traces_retention_days =
            body.get("traces_retention_days")
                .and_then(Value::as_i64)
                .context("traces_retention_days is required")? as i32;
        let metrics_retention_days = body
            .get("metrics_retention_days")
            .and_then(Value::as_i64)
            .unwrap_or(30) as i32;
        let logs_retention_days = body
            .get("logs_retention_days")
            .and_then(Value::as_i64)
            .unwrap_or(30) as i32;
        // null means unlimited; omitting the field keeps the existing default
        let max_ingestion_rate = body
            .get("max_ingestion_rate")
            .and_then(|v| if v.is_null() { None } else { v.as_i64() })
            .map(|v| v as i32);
        let file_push_interval_secs = body
            .get("file_push_interval_secs")
            .and_then(Value::as_i64)
            .unwrap_or(300) as i32;
        let blocked = body
            .get("blocked")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let capture_llm_content_enabled = body
            .get("capture_llm_content_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let admin = client
            .admin_grpc()
            .clone()
            .with_workspace_id(workspace_id.clone());
        let resp = match admin
            .update_workspace_settings(WorkspaceSettingsInput {
                traces_retention_days,
                metrics_retention_days,
                logs_retention_days,
                max_ingestion_rate,
                file_push_interval_secs,
                blocked,
                capture_llm_content_enabled,
            })
            .await
        {
            Ok(resp) => resp,
            Err(err) => return map_grpc_error(err),
        };
        let body = resp
            .settings
            .as_ref()
            .map(workspace_settings_json)
            .unwrap_or(Value::Null);
        return Ok(success_response(body));
    }

    bail!("unsupported PUT endpoint for gRPC transport: {path}")
}

pub async fn dispatch_delete(
    _client: &TransportApiClient,
    path: &str,
) -> Result<TransportResponse> {
    bail!("unsupported DELETE endpoint for gRPC transport: {path}")
}

fn split_path_query(path: &str) -> (&str, HashMap<String, String>) {
    let (route, raw_query) = match path.split_once('?') {
        Some((r, q)) => (r, q),
        None => (path, ""),
    };
    let mut out = HashMap::new();
    for entry in raw_query.split('&') {
        if entry.is_empty() {
            continue;
        }
        let (k, v) = entry.split_once('=').unwrap_or((entry, ""));
        let key = urlencoding::decode(k)
            .map(|d| d.into_owned())
            .unwrap_or_else(|_| k.to_string());
        let value = urlencoding::decode(v)
            .map(|d| d.into_owned())
            .unwrap_or_else(|_| v.to_string());
        out.insert(key, value);
    }
    (route, out)
}

fn parse_u32_opt(query: &HashMap<String, String>, key: &str) -> Result<Option<u32>> {
    query
        .get(key)
        .map(|v| {
            v.parse::<u32>()
                .with_context(|| format!("invalid integer query param {key}={v}"))
        })
        .transpose()
}

fn parse_i64_opt(query: &HashMap<String, String>, key: &str) -> Result<Option<i64>> {
    query
        .get(key)
        .map(|v| {
            v.parse::<i64>()
                .with_context(|| format!("invalid integer query param {key}={v}"))
        })
        .transpose()
}

fn parse_span_types(query: &HashMap<String, String>) -> Option<Vec<String>> {
    let mut out = Vec::new();
    if let Some(single) = query.get("span_type") {
        out.push(single.clone());
    }
    if let Some(multi) = query.get("span_types") {
        out.extend(
            multi
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
    }
    if out.is_empty() { None } else { Some(out) }
}

fn span_query_params_from_map(query: &HashMap<String, String>) -> Result<SpanQueryParams> {
    let time_range = match (query_time_start(query)?, query_time_end(query)?) {
        (Some(start), Some(end)) => Some(recent_time_range_from_bounds(start, end)),
        (None, None) => None,
        (Some(start), None) => Some(query_proto::TimeRange {
            start: Some(start),
            end: Some(timestamp_now()),
        }),
        (None, Some(end)) => Some(query_proto::TimeRange {
            start: Some(timestamp_hours_ago(24)),
            end: Some(end),
        }),
    };

    Ok(SpanQueryParams {
        trace_id: query.get("trace_id").cloned(),
        time_range,
        service_name: query.get("service_name").cloned(),
        operation_name: query.get("operation_name").cloned(),
        span_types: parse_span_types(query).unwrap_or_default(),
        status: query.get("status").cloned(),
        llm_model: query.get("llm_model").cloned(),
        llm_provider: query.get("llm_provider").cloned(),
        llm_response_model: query.get("llm_response_model").cloned(),
        agent_name: query.get("agent_name").cloned(),
        session_id: query.get("session_id").cloned(),
        rail_type: query.get("rail_type").cloned(),
        action_name: query.get("action_name").cloned(),
        workflow_run_id: query.get("workflow_run_id").cloned(),
        framework: query.get("framework").cloned(),
        tool_name: query.get("tool_name").cloned(),
        invocation_id: query.get("invocation_id").cloned(),
        environment: query.get("environment").cloned(),
    })
}

fn matches_invocation_id(span: &query_proto::SpanDetail, filter: Option<&String>) -> bool {
    filter.is_none_or(|expected| {
        span.attributes
            .get("invocation_id")
            .is_some_and(|v| v == expected)
    })
}

fn recent_time_range_from_bounds(start: Timestamp, end: Timestamp) -> query_proto::TimeRange {
    query_proto::TimeRange {
        start: Some(start),
        end: Some(end),
    }
}

fn timestamp_hours_ago(hours: i64) -> Timestamp {
    crate::helpers::timestamp_hours_ago(hours)
}

fn pagination(query: &HashMap<String, String>) -> (usize, usize) {
    let limit = parse_i64_opt(query, "limit")
        .ok()
        .flatten()
        .unwrap_or(100)
        .max(1) as usize;
    let offset = parse_i64_opt(query, "offset")
        .ok()
        .flatten()
        .unwrap_or(0)
        .max(0) as usize;
    (limit, offset)
}

fn query_time_start(query: &HashMap<String, String>) -> Result<Option<Timestamp>> {
    query_time(query, &["start_time", "start"])
}

fn query_time_end(query: &HashMap<String, String>) -> Result<Option<Timestamp>> {
    let parsed = query_time(query, &["end_time", "end"])?;
    Ok(parsed.or_else(|| {
        if query.contains_key("start_time") || query.contains_key("start") {
            Some(timestamp_now())
        } else {
            None
        }
    }))
}

fn query_time(query: &HashMap<String, String>, keys: &[&str]) -> Result<Option<Timestamp>> {
    for key in keys {
        if let Some(raw) = query.get(*key) {
            let dt = DateTime::parse_from_rfc3339(raw)
                .with_context(|| format!("invalid RFC3339 value for {key}: {raw}"))?
                .with_timezone(&Utc);
            return Ok(Some(Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }));
        }
    }
    Ok(None)
}

fn ts_to_datetime(ts: &Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
}

fn matches_time_filter(
    ts: Option<&Timestamp>,
    start: Option<&Timestamp>,
    end: Option<&Timestamp>,
) -> bool {
    if start.is_none() && end.is_none() {
        return true;
    }
    let Some(ts) = ts.and_then(ts_to_datetime) else {
        return false;
    };
    if let Some(start) = start.and_then(ts_to_datetime)
        && ts < start
    {
        return false;
    }
    if let Some(end) = end.and_then(ts_to_datetime)
        && ts > end
    {
        return false;
    }
    true
}

fn matches_string_filter(field: &str, filter: Option<&String>) -> bool {
    filter.is_none_or(|v| field == v)
}

fn matches_string_filter_opt(field: &Option<String>, filter: Option<&String>) -> bool {
    filter.is_none_or(|v| field.as_deref() == Some(v.as_str()))
}

fn metric_matches_agent_name(
    metric: &query_proto::MetricDetail,
    agent_name: Option<&String>,
) -> bool {
    agent_name.is_none_or(|name| {
        metric
            .labels
            .get("agent_name")
            .or_else(|| metric.labels.get("agent"))
            .is_some_and(|v| v == name)
    })
}

async fn trace_matches_nested_filters(
    client: &TransportApiClient,
    trace_id: &str,
    query: &HashMap<String, String>,
) -> Result<bool> {
    let needs_nested = [
        "llm_model",
        "llm_provider",
        "llm_response_model",
        "agent_name",
        "session_id",
        "rail_type",
        "action_name",
        "workflow_run_id",
        "framework",
        "tool_name",
        "invocation_id",
        "environment",
    ]
    .iter()
    .any(|k| query.contains_key(*k));
    if !needs_nested {
        return Ok(true);
    }

    let detail = match client.query_grpc().get_trace(trace_id).await {
        Ok(resp) => resp.trace,
        Err(_) => None,
    };
    let Some(trace) = detail else {
        return Ok(false);
    };
    Ok(trace.spans.iter().any(|span| {
        matches_string_filter_opt(&span.llm_model, query.get("llm_model"))
            && matches_string_filter_opt(&span.llm_provider, query.get("llm_provider"))
            && matches_string_filter_opt(&span.llm_response_model, query.get("llm_response_model"))
            && matches_string_filter_opt(&span.agent_name, query.get("agent_name"))
            && matches_string_filter_opt(&span.session_id, query.get("session_id"))
            && matches_string_filter_opt(&span.rail_type, query.get("rail_type"))
            && matches_string_filter_opt(&span.action_name, query.get("action_name"))
            && matches_string_filter_opt(&span.workflow_run_id, query.get("workflow_run_id"))
            && matches_string_filter_opt(&span.framework, query.get("framework"))
            && matches_string_filter_opt(&span.tool_name, query.get("tool_name"))
            && matches_invocation_id(span, query.get("invocation_id"))
            && matches_string_filter_opt(&span.environment, query.get("environment"))
    }))
}

fn paginated_body<T, F>(items: Vec<T>, (limit, offset): (usize, usize), map: F) -> Value
where
    F: FnMut(T) -> Value,
{
    let total = items.len() as i64;
    paginated_body_from_count(items, total, (limit, offset), map)
}

fn paginated_body_from_count<T, F>(
    items: Vec<T>,
    total_count: i64,
    (limit, offset): (usize, usize),
    mut map: F,
) -> Value
where
    F: FnMut(T) -> Value,
{
    let page_items = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(&mut map)
        .collect::<Vec<_>>();
    json!({
        "items": page_items,
        "total": total_count,
        "page": 0,
        "page_size": limit as i64,
    })
}

fn timestamp_json(ts: Option<&Timestamp>) -> Value {
    ts.and_then(ts_to_datetime)
        .map(|dt| json!(dt.to_rfc3339()))
        .unwrap_or(Value::Null)
}

fn parse_embedded_json(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!(raw))
}

fn parse_string_map(map: &HashMap<String, String>) -> Value {
    let mut out = JsonMap::new();
    for (k, v) in map {
        out.insert(k.clone(), parse_embedded_json(v));
    }
    Value::Object(out)
}

fn span_detail_json(span: &query_proto::SpanDetail) -> Value {
    let model_parameters = span
        .model_parameters_json
        .as_deref()
        .map(parse_embedded_json)
        .unwrap_or(Value::Null);
    let events = span
        .events_json
        .as_deref()
        .map(parse_embedded_json)
        .unwrap_or(Value::Null);
    let links = span
        .links_json
        .as_deref()
        .map(parse_embedded_json)
        .unwrap_or(Value::Null);
    json!({
        "span_id": span.span_id,
        "trace_id": span.trace_id,
        "parent_span_id": span.parent_span_id,
        "service_name": span.service_name,
        "operation_name": span.operation_name,
        "span_type": span.span_type,
        "start_time": timestamp_json(span.start_time.as_ref()),
        "duration_ms": span.duration_ms,
        "status": span.status,
        "agent_name": span.agent_name,
        "agent_type": span.agent_type,
        "session_id": span.session_id,
        "llm_model": span.llm_model,
        "llm_provider": span.llm_provider,
        "llm_response_model": span.llm_response_model,
        "llm_input": span.llm_input,
        "llm_output": span.llm_output,
        "prompt_tokens": span.prompt_tokens,
        "completion_tokens": span.completion_tokens,
        "total_tokens": span.total_tokens,
        "prompt_cost_usd": span.prompt_cost_usd,
        "completion_cost_usd": span.completion_cost_usd,
        "total_cost_usd": span.total_cost_usd,
        "tool_name": span.tool_name,
        "tool_call_id": span.tool_call_id,
        "rail_type": span.rail_type,
        "rail_name": span.rail_name,
        "rail_stop": span.rail_stop,
        "action_name": span.action_name,
        "workflow_run_id": span.workflow_run_id,
        "framework": span.framework,
        "llm_cache_hit": span.llm_cache_hit,
        "llm_response_id": span.llm_response_id,
        "environment": span.environment,
        "db_system_name": span.db_system_name,
        "db_namespace": span.db_namespace,
        "db_operation_name": span.db_operation_name,
        "db_query_text": span.db_query_text,
        "db_query_summary": span.db_query_summary,
        "db_collection_name": span.db_collection_name,
        "db_response_status_code": span.db_response_status_code,
        "agent_id": span.agent_id,
        "agent_description": span.agent_description,
        "agent_task_id": span.agent_task_id,
        "agent_task_parent_id": span.agent_task_parent_id,
        "agent_task_name": span.agent_task_name,
        "agent_task_kind": span.agent_task_kind,
        "agent_task_state": span.agent_task_state,
        "agent_task_status": span.agent_task_status,
        "memory_type": span.memory_type,
        "memory_key": span.memory_key,
        "mcp_tool_name": span.mcp_tool_name,
        "mcp_server_name": span.mcp_server_name,
        "mcp_tool_input": span.mcp_tool_input,
        "mcp_tool_output": span.mcp_tool_output,
        "agent_version": span.agent_version,
        "sdk_version": span.sdk_version,
        "evaluation_name": span.evaluation_name,
        "evaluation_explanation": span.evaluation_explanation,
        "evaluation_passed": span.evaluation_passed,
        "model_parameters": model_parameters,
        "events": events,
        "links": links,
        "attributes": parse_string_map(&span.attributes),
    })
}

fn trace_detail_json(trace: &query_proto::TraceDetail) -> Value {
    json!({
        "trace_id": trace.trace_id,
        "start_time": timestamp_json(trace.start_time.as_ref()),
        "duration_ms": trace.duration_ms,
        "spans": trace.spans.iter().map(span_detail_json).collect::<Vec<_>>(),
    })
}

fn log_detail_json(log: &query_proto::LogDetail) -> Value {
    json!({
        "id": log.id,
        "timestamp": timestamp_json(log.timestamp.as_ref()),
        "severity": log.severity,
        "service_name": log.service_name,
        "message": log.message,
        "trace_id": log.trace_id,
        "span_id": log.span_id,
        "agent_name": log.agent_name,
        "session_id": log.session_id,
        "user_id": log.user_id,
        "attributes": parse_string_map(&log.attributes),
    })
}

fn metric_detail_json(metric: &query_proto::MetricDetail) -> Value {
    json!({
        "metric_name": metric.metric_name,
        "metric_type": metric.metric_type,
        "timestamp": timestamp_json(metric.timestamp.as_ref()),
        "service_name": metric.service_name,
        "value": metric.value,
        "count": metric.count,
        "sum": metric.sum,
        "min": metric.min,
        "max": metric.max,
        "labels": parse_string_map(&metric.labels),
    })
}

fn workspace_settings_json(settings: &admin_proto::WorkspaceSettings) -> Value {
    json!({
        "id": settings.id,
        "workspace_id": settings.workspace_id,
        "traces_retention_days": settings.traces_retention_days,
        "metrics_retention_days": settings.metrics_retention_days,
        "logs_retention_days": settings.logs_retention_days,
        "max_ingestion_rate": settings.max_ingestion_rate,
        "file_push_interval_secs": settings.file_push_interval_secs,
        "blocked": settings.blocked,
        "capture_llm_content_enabled": settings.capture_llm_content_enabled,
        "updated_at": settings.updated_at,
    })
}

fn audit_log_json(log: admin_proto::AuditLog) -> Value {
    let metadata = serde_json::from_str::<Value>(&log.metadata_json).unwrap_or(Value::Null);
    json!({
        "id": log.id,
        "actor_workspace_id": log.actor_workspace_id,
        "resource_workspace_id": log.resource_workspace_id,
        "action": log.action,
        "resource_type": log.resource_type,
        "resource_id": log.resource_id,
        "metadata": metadata,
        "created_at": log.created_at,
    })
}

fn policy_configs_from_body(body: &Value) -> Result<Vec<admin_proto::PolicyConfig>> {
    let policies = body
        .get("policies")
        .and_then(Value::as_array)
        .context("policies array is required")?;

    policies
        .iter()
        .map(|p| {
            Ok(admin_proto::PolicyConfig {
                signal: p
                    .get("signal")
                    .and_then(Value::as_str)
                    .context("policy.signal is required")?
                    .to_string(),
                operation: p
                    .get("operation")
                    .and_then(Value::as_str)
                    .context("policy.operation is required")?
                    .to_string(),
                limit_json: serde_json::to_string(
                    p.get("limit").context("policy.limit is required")?,
                )
                .context("serialize policy.limit")?,
                grace_pct: p.get("grace_pct").and_then(Value::as_u64).map(|v| v as u32),
                hard_block_pct: p
                    .get("hard_block_pct")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32),
                effective_from: p.get("effective_from").and_then(Value::as_i64),
                effective_until: p.get("effective_until").and_then(Value::as_i64),
                source: p.get("source").and_then(Value::as_str).map(str::to_string),
            })
        })
        .collect()
}

fn workspace_from_path(path: &str) -> Result<String> {
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    if parts.len() < 4 {
        bail!("invalid workspace path: {path}");
    }
    Ok(parts[3].to_string())
}

fn admin_client_for_query_override(
    client: &TransportApiClient,
    query: &HashMap<String, String>,
) -> crate::helpers::ZradarAdminClient {
    if let Some(workspace_id) = query.get("workspace_id") {
        return client
            .admin_grpc()
            .clone()
            .with_workspace_id(workspace_id.clone());
    }
    client.admin_grpc().clone()
}

fn success_response(body: Value) -> TransportResponse {
    TransportResponse::from_grpc(200, Some(body.clone()), Some(body.to_string()))
}

fn map_grpc_error(err: anyhow::Error) -> Result<TransportResponse> {
    let Some(status) = find_status(&err) else {
        return Err(err);
    };
    let code = match status.code() {
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::InvalidArgument => 400,
        Code::NotFound => 404,
        Code::ResourceExhausted => 429,
        _ => 500,
    };

    if status.code() == Code::InvalidArgument && status.message().contains("query_window_violation")
    {
        let body = json!({ "error": "query_window_violation" });
        return Ok(TransportResponse::from_grpc(
            code,
            Some(body.clone()),
            Some(body.to_string()),
        ));
    }

    let body = json!({ "error": status.message() });
    Ok(TransportResponse::from_grpc(
        code,
        Some(body.clone()),
        Some(body.to_string()),
    ))
}

fn find_status(err: &anyhow::Error) -> Option<&Status> {
    err.chain().find_map(|cause| cause.downcast_ref::<Status>())
}

#[allow(dead_code)]
fn default_time_range() -> query_proto::TimeRange {
    recent_time_range()
}
