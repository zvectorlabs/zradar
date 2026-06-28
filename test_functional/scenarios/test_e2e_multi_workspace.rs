//! Phase 5 multi-workspace isolation suite (TECH-SPEC-PHASE-5 §5, OQ30).
//!
//! Goal: for every isolation axis the API exposes, prove that Tenant A's
//! filter query cannot see Tenant B's data. Empty-result assertions are
//! first-class — silent leakage is the failure mode this suite is
//! preventing.
//!
//! Tenant fan-out uses the existing `x-workspace-id` test-header mechanism
//! (`allow_test_header_context = true` in `config.test.toml`): every
//! `TestEnv::setup()` returns a fresh workspace_id, so two parallel envs are
//! genuinely independent.
//!
//! Each axis follows the same shape:
//! 1. Ingest a span under workspace A with `axis = ALPHA_VALUE`.
//! 2. Ingest a span under workspace B with `axis = BRAVO_VALUE`.
//! 3. Wait until both spans land.
//! 4. Query `?axis=ALPHA_VALUE` from A → expect at least one hit, all
//!    tagged with workspace A's workspace_id.
//! 5. Query `?axis=ALPHA_VALUE` from B → expect zero hits (B never used
//!    that value).
//! 6. Query `?axis=BRAVO_VALUE` from A → expect zero hits.
//! 7. Query `?axis=BRAVO_VALUE` from B → expect at least one hit.
//!
//! Axes covered today (the 8 server-side-filterable ones):
//! - rail_type, action_name, workflow_run_id, framework
//! - tool_name, invocation_id, llm_response_model, environment
//!
//! Not covered (left for future work):
//! - `llm_cache_hit`: filterable column doesn't exist on SpanQueryFilters
//!   today, only readable on SpanDetail.
//! - `links`: same — readable, not filterable.
//! - `scores`: rides through `/api/v1/logs?trace_id=...` (evaluator emits
//!   scores as logs). Covered by `test_score_log_workspace_isolation`.

#[allow(unused_imports)]
use crate::*;

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
use serde_json::Value;
use uuid::Uuid;

// ===========================================================================
// Helpers
// ===========================================================================

fn kv_str(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(OtlpAnyValue {
            value: Some(AnyValue::StringValue(v.to_string())),
        }),
    }
}

/// Build a single-span ResourceSpans payload tagged with `service_name`,
/// `environment`, and the caller-supplied `attrs`. The trace_id and span_id
/// are random so back-to-back calls within one test never collide.
fn build_resource_spans(
    service_name: &str,
    environment: &str,
    attrs: Vec<KeyValue>,
) -> ResourceSpans {
    let trace_id = Uuid::new_v4().as_bytes().to_vec();
    let mut span_id = vec![0u8; 8];
    span_id.copy_from_slice(&Uuid::new_v4().as_bytes()[0..8]);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    ResourceSpans {
        resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![
                kv_str("service.name", service_name),
                kv_str("deployment.environment", environment),
            ],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans: vec![OtlpSpan {
                trace_id,
                span_id,
                name: "isolation.probe".to_string(),
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 1_000_000,
                attributes: attrs,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Ingest one span via OTLP/gRPC.
async fn ingest(
    env: &TestEnv,
    service: &str,
    environment: &str,
    attrs: Vec<KeyValue>,
) -> Result<()> {
    let resource_spans = build_resource_spans(service, environment, attrs);
    let req = opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    };
    env.otlp.export_traces(req).await
}

/// Wait for a /spans query to return at least one item. The real isolation
/// proof is `assert_empty_for` — if workspace B queries workspace A's axis value
/// and gets zero rows, isolation holds. The positive assertion here only
/// confirms ingest completed (so the negative assertion is meaningful).
///
/// Note: `/api/v1/spans` items do not expose `workspace_id` on each row, so
/// we cannot assert workspace ownership here. Isolation is validated by the
/// cross-workspace empty assertions in `run_axis_isolation`.
async fn wait_until_visible(client: &ApiClient, url: &str, ctx: &str) -> Result<Vec<Value>> {
    wait_for_items_default(client, url)
        .await
        .with_context(|| format!("{ctx}: GET {url} did not return any items"))
}

/// Query a /spans URL once and assert zero items. We do *not* poll here —
/// the positive `wait_until_visible` calls above already established that
/// ingest is complete on both workspaces, so any leakage would already be
/// observable.
async fn assert_empty_for(client: &ApiClient, url: &str, ctx: &str) -> Result<()> {
    let resp = client.get(url).await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("{ctx}: GET {url} returned {status}: {body}");
    }
    let data: Value = resp.json().await?;
    let items_len = data
        .get("items")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    if items_len > 0 {
        anyhow::bail!("{ctx}: cross-workspace query leaked {items_len} item(s) (URL: {url})");
    }
    Ok(())
}

/// One pass of the cross-workspace matrix on a single filterable attribute.
///
/// - `axis_attr_key`: OTLP attribute key the span carries (e.g. "rail.type").
/// - `axis_filter_param`: query-param name (e.g. "rail_type") — usually the
///   attribute key with dots replaced by underscores.
/// - `alpha_value`: value workspace A's span will carry.
/// - `bravo_value`: value workspace B's span will carry.
///
/// Extra resource-attribute pairs (`extra_attrs_a`, `extra_attrs_b`) cover
/// axes where the attribute lives in OTLP outside the simple
/// "workspace-A puts X, workspace-B puts Y" pattern — e.g. `environment` rides on
/// the Resource, not the Span's attributes.
async fn run_axis_isolation(
    axis_filter_param: &str,
    alpha_value: &str,
    bravo_value: &str,
    span_attrs_a: Vec<KeyValue>,
    span_attrs_b: Vec<KeyValue>,
    env_a_env: &str,
    env_b_env: &str,
) -> Result<()> {
    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;
    assert_ne!(
        env_a.workspace_id, env_b.workspace_id,
        "TestEnv must give independent workspace_ids"
    );

    let service_a = format!("isolation-svc-a-{axis_filter_param}");
    let service_b = format!("isolation-svc-b-{axis_filter_param}");

    ingest(&env_a, &service_a, env_a_env, span_attrs_a).await?;
    ingest(&env_b, &service_b, env_b_env, span_attrs_b).await?;

    let url_alpha = format!(
        "/api/v1/spans?{axis_filter_param}={}",
        urlencoding::encode(alpha_value)
    );
    let url_bravo = format!(
        "/api/v1/spans?{axis_filter_param}={}",
        urlencoding::encode(bravo_value)
    );

    // Positive: each workspace sees its own row(s).
    wait_until_visible(
        &env_a.client,
        &url_alpha,
        &format!("A queries own axis {axis_filter_param}={alpha_value}"),
    )
    .await?;
    wait_until_visible(
        &env_b.client,
        &url_bravo,
        &format!("B queries own axis {axis_filter_param}={bravo_value}"),
    )
    .await?;

    // Negative: cross-workspace queries return nothing.
    assert_empty_for(
        &env_b.client,
        &url_alpha,
        &format!("B queries A's axis {axis_filter_param}={alpha_value}"),
    )
    .await?;
    assert_empty_for(
        &env_a.client,
        &url_bravo,
        &format!("A queries B's axis {axis_filter_param}={bravo_value}"),
    )
    .await?;

    Ok(())
}

// ===========================================================================
// Per-axis isolation tests
// ===========================================================================

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_rail_type() -> Result<()> {
    run_axis_isolation(
        "rail_type",
        "input",
        "output",
        vec![kv_str("rail.type", "input")],
        vec![kv_str("rail.type", "output")],
        "test-rail-a",
        "test-rail-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_action_name() -> Result<()> {
    run_axis_isolation(
        "action_name",
        "summarize_alpha",
        "summarize_bravo",
        vec![kv_str("action.name", "summarize_alpha")],
        vec![kv_str("action.name", "summarize_bravo")],
        "test-action-a",
        "test-action-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_workflow_run_id() -> Result<()> {
    run_axis_isolation(
        "workflow_run_id",
        "wf-alpha-iso",
        "wf-bravo-iso",
        vec![kv_str("nat.workflow.run_id", "wf-alpha-iso")],
        vec![kv_str("nat.workflow.run_id", "wf-bravo-iso")],
        "test-wf-a",
        "test-wf-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_framework() -> Result<()> {
    run_axis_isolation(
        "framework",
        "langchain_alpha",
        "langgraph_bravo",
        vec![kv_str("nat.framework", "langchain_alpha")],
        vec![kv_str("nat.framework", "langgraph_bravo")],
        "test-fw-a",
        "test-fw-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_tool_name() -> Result<()> {
    run_axis_isolation(
        "tool_name",
        "calculator_alpha",
        "websearch_bravo",
        vec![kv_str("tool.name", "calculator_alpha")],
        vec![kv_str("tool.name", "websearch_bravo")],
        "test-tool-a",
        "test-tool-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_invocation_id() -> Result<()> {
    run_axis_isolation(
        "invocation_id",
        "inv-alpha-001",
        "inv-bravo-001",
        vec![kv_str("invocation.id", "inv-alpha-001")],
        vec![kv_str("invocation.id", "inv-bravo-001")],
        "test-inv-a",
        "test-inv-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_llm_response_model() -> Result<()> {
    run_axis_isolation(
        "llm_response_model",
        "gpt-4-alpha",
        "gpt-4-bravo",
        vec![kv_str("gen_ai.response.model", "gpt-4-alpha")],
        vec![kv_str("gen_ai.response.model", "gpt-4-bravo")],
        "test-llm-a",
        "test-llm-b",
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_workspace_isolation_environment() -> Result<()> {
    // `environment` is a *resource* attribute (deployment.environment), not a
    // span attribute. The helper already places it on the Resource; the
    // span_attrs vectors stay empty.
    run_axis_isolation(
        "environment",
        "prod-alpha",
        "prod-bravo",
        vec![],
        vec![],
        "prod-alpha",
        "prod-bravo",
    )
    .await
}

// ===========================================================================
// Coarse-grained baseline — no filter at all
// ===========================================================================

/// Sanity baseline (and the assertion that mirrors §5's first arrow): even
/// without any axis filter, workspace A's traces must not appear in workspace B's
/// `/api/v1/traces` listing. This guards against bugs in the very base workspace
/// filter — every per-axis assertion above implicitly depends on this.
#[tokio::test]
#[ignore]
async fn test_workspace_isolation_baseline_traces_listing() -> Result<()> {
    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;
    assert_ne!(env_a.workspace_id, env_b.workspace_id);

    ingest(
        &env_a,
        "baseline-a",
        "iso-a",
        vec![kv_str("scenario", "baseline")],
    )
    .await?;
    // Don't ingest under B — that way, *every* trace returned from B's
    // /traces must necessarily be a leak.

    // Wait for A's trace to appear under A.
    let _ = wait_for_items_default(&env_a.client, "/api/v1/traces")
        .await
        .context("A must see its own trace in /traces")?;

    // B's /traces must be empty. If a /traces leak occurred we'd see at
    // least one row tagged with A's workspace.
    let resp = env_b.client.get("/api/v1/traces").await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!(
            "GET /api/v1/traces (as B) returned {status}: {}",
            resp.text().await.unwrap_or_default()
        );
    }
    let data: Value = resp.json().await?;
    let leaked = data
        .get("items")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    if leaked > 0 {
        anyhow::bail!(
            "Baseline cross-workspace leak: B's /traces shows {leaked} row(s) while only A ingested"
        );
    }
    Ok(())
}

// ===========================================================================
// Logs / scores workspace isolation
// ===========================================================================

/// Evaluator scores ride through `/api/v1/logs` (an evaluator emits each
/// score as an OTLP log). Cross-workspace queries must not surface another
/// workspace's score logs.
#[tokio::test]
#[ignore]
async fn test_score_log_workspace_isolation() -> Result<()> {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;
    assert_ne!(env_a.workspace_id, env_b.workspace_id);

    let trace_id = Uuid::new_v4().as_bytes().to_vec();
    let span_id = Uuid::new_v4().as_bytes()[..8].to_vec();
    let trace_id_hex = hex::encode(&trace_id);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let log_record = LogRecord {
        time_unix_nano: now_ns,
        observed_time_unix_nano: now_ns + 100,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: Some(OtlpAnyValue {
            value: Some(AnyValue::StringValue("score=0.91".to_string())),
        }),
        attributes: vec![
            kv_str("nemo.evaluator.name", "answer_relevance"),
            kv_str("score.type", "numeric"),
        ],
        trace_id,
        span_id,
        ..Default::default()
    };

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![kv_str("service.name", "nemo-evaluator")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![log_record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    env_a.otlp.export_logs(req).await?;

    // A sees its score log.
    let a_url = format!("/api/v1/logs?trace_id={trace_id_hex}");
    let _ = wait_for_items_default(&env_a.client, &a_url)
        .await
        .context("A must see its own evaluator log")?;

    // B queries the same trace_id and must see zero.
    let resp = env_b.client.get(&a_url).await?;
    let data: Value = resp.json().await?;
    let leaked = data
        .get("items")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    if leaked > 0 {
        anyhow::bail!(
            "Evaluator-log cross-workspace leak: B's query for A's trace_id={trace_id_hex} returned {leaked} row(s)"
        );
    }
    Ok(())
}
