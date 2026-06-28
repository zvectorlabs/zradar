//! Phase 2 NeMo analytics + filters functional tests (R2.1 – R2.4).
//!
//! Each test validates a specific acceptance criterion. All tests are
//! `#[ignore]` and run only against a live stack: `make functional_tests`.

#[allow(unused_imports)]
use crate::*;
use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

// ---------------------------------------------------------------------------
// Attribute construction helpers (duplicated from phase1 for self-containment)
// ---------------------------------------------------------------------------

fn av_str(val: &str) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(AnyValue::StringValue(val.to_string())),
    }
}

fn av_int(val: i64) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(AnyValue::IntValue(val)),
    }
}

fn av_bool(val: bool) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(AnyValue::BoolValue(val)),
    }
}

fn kv_str(key: &str, val: &str) -> (String, OtlpAnyValue) {
    (key.to_string(), av_str(val))
}

#[allow(dead_code)]
fn kv_int(key: &str, val: i64) -> (String, OtlpAnyValue) {
    (key.to_string(), av_int(val))
}

fn kv_bool(key: &str, val: bool) -> (String, OtlpAnyValue) {
    (key.to_string(), av_bool(val))
}

// ---------------------------------------------------------------------------
// AC2.1 — rail_type filter
// ---------------------------------------------------------------------------

/// AC2.1a: ?rail_type=input returns only spans with rail_type='input'.
#[tokio::test]
#[ignore]
async fn test_r2_1_rail_type_filter_returns_matching() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();

    // Ingest two GUARDRAIL spans: one with rail_type=input, one with rail_type=output
    let trace_id_input = TestDataGenerator::trace_id();
    let span_id_input = TestDataGenerator::span_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id_input,
            vec![SpanDefExt {
                name: "guardrails.rail".to_string(),
                span_id: span_id_input,
                parent_span_id: None,
                attributes: vec![
                    kv_str("rail.type", "input"),
                    kv_str("rail.name", "self_check_input"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_output = TestDataGenerator::trace_id();
    let span_id_output = TestDataGenerator::span_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id_output,
            vec![SpanDefExt {
                name: "guardrails.rail".to_string(),
                span_id: span_id_output,
                parent_span_id: None,
                attributes: vec![
                    kv_str("rail.type", "output"),
                    kv_str("rail.name", "self_check_output"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    // Wait for both traces to be queryable
    let input_hex = hex::encode(trace_id_input);
    let output_hex = hex::encode(trace_id_output);
    wait_for_trace_default(&env.client, &input_hex).await?;
    wait_for_trace_default(&env.client, &output_hex).await?;

    // Filter by rail_type=input — should return the input span only
    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&rail_type=input"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    for item in items {
        assert_eq!(
            item["rail_type"], "input",
            "Filter ?rail_type=input must return only input spans"
        );
    }
    assert!(
        !items.is_empty(),
        "?rail_type=input must return at least one span"
    );
    println!("✅ AC2.1a: ?rail_type=input returns only input guardrail spans");
    Ok(())
}

/// AC2.1 edge case: filter that matches nothing returns empty items, not 200-with-everything.
#[tokio::test]
#[ignore]
async fn test_r2_1_filter_no_match_returns_empty() -> Result<()> {
    let env = TestEnv::setup().await?;
    let workspace_id = env.client.workspace_id();

    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&rail_type=nonexistent_type_zzz"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let total = data["total"].as_i64().unwrap_or(-1);
    assert_eq!(
        total, 0,
        "Unmatched filter must return total=0, not all rows"
    );
    println!("✅ AC2.11: Empty result from non-matching filter has total=0");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.2 — action_name filter
// ---------------------------------------------------------------------------

/// AC2.2: ?action_name=self_check_input filters correctly.
#[tokio::test]
#[ignore]
async fn test_r2_2_action_name_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "guardrails.action".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("action.name", "self_check_input")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&action_name=self_check_input"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(
        !items.is_empty(),
        "?action_name=self_check_input must return at least one span"
    );
    for item in items {
        assert_eq!(item["action_name"], "self_check_input");
    }
    println!("✅ AC2.2: ?action_name filter works correctly");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.3 — workflow_run_id filter
// ---------------------------------------------------------------------------

/// AC2.3: ?workflow_run_id=... filters to one NAT workflow.
#[tokio::test]
#[ignore]
async fn test_r2_3_workflow_run_id_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let unique_run_id = format!("wf-{}", hex::encode(TestDataGenerator::trace_id()));

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "nat.workflow".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("nat.workflow.run_id", &unique_run_id)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&workflow_run_id={unique_run_id}"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(!items.is_empty(), "workflow_run_id filter must return span");
    assert_eq!(items[0]["workflow_run_id"], unique_run_id);
    println!("✅ AC2.3: ?workflow_run_id filter returns the correct NAT workflow span");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.4 — framework filter
// ---------------------------------------------------------------------------

/// AC2.4: ?framework=langchain filters correctly.
#[tokio::test]
#[ignore]
async fn test_r2_4_framework_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "chain.run".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("nat.framework", "langchain")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&framework=langchain"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(!items.is_empty(), "?framework=langchain must return span");
    for item in items {
        assert_eq!(item["framework"], "langchain");
    }
    println!("✅ AC2.4: ?framework filter works correctly");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.5 — tool_name filter
// ---------------------------------------------------------------------------

/// AC2.5: ?tool_name=web_search filters correctly.
#[tokio::test]
#[ignore]
async fn test_r2_5_tool_name_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "tool.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("tool.name", "web_search")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&tool_name=web_search"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(!items.is_empty(), "?tool_name=web_search must return span");
    for item in items {
        assert_eq!(item["tool_name"], "web_search");
    }
    println!("✅ AC2.5: ?tool_name filter works correctly");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.6 — invocation_id filter
// ---------------------------------------------------------------------------

/// AC2.6: ?invocation_id=... filters correctly with the canonical wire key
/// `invocation.id` (P2-G3: AgentConvention now maps invocation.id /
/// zradar.invocation.id / invocation_id into the invocation_id column).
#[tokio::test]
#[ignore]
async fn test_r2_6_invocation_id_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let unique_inv_id = format!("inv-{}", hex::encode(TestDataGenerator::span_id()));

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "agent.invoke".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("invocation.id", &unique_inv_id)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let path = format!("/api/v1/spans?workspace_id={workspace_id}&invocation_id={unique_inv_id}");
    let data = poll_until(
        || {
            let client = &env.client;
            let path = path.clone();
            let trace_id_hex = trace_id_hex.clone();
            async move {
                let resp = client.get(&path).await?;
                assert!(resp.status().is_success());
                let data: serde_json::Value = resp.json().await?;
                let items = data["items"].as_array().expect("items array");
                if items.iter().any(|item| item["trace_id"] == trace_id_hex) {
                    Ok(Some(data))
                } else {
                    Ok(None)
                }
            }
        },
        Duration::from_secs(5),
        Duration::from_millis(200),
    )
    .await?;
    assert!(
        data["items"]
            .as_array()
            .expect("items array")
            .iter()
            .any(|item| item["trace_id"] == trace_id_hex),
        "?invocation_id must return the ingested span (wire key invocation.id → invocation_id)"
    );
    println!("✅ AC2.6: ?invocation_id filter returns the matching span");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.7 / AC2.8 — RERANKER span type
// ---------------------------------------------------------------------------

/// AC2.7: Span with openinference.span.kind=RERANKER is stored as span_type='RERANKER'.
#[tokio::test]
#[ignore]
async fn test_r2_7_reranker_span_type_detection() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "reranker.run".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("openinference.span.kind", "RERANKER")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["span_type"], "RERANKER",
        "openinference.span.kind=RERANKER must produce span_type=RERANKER"
    );
    println!("✅ AC2.7: Reranker span correctly typed as RERANKER");
    Ok(())
}

/// AC2.8: RERANKER spans are returned by ?span_type=RERANKER filter.
#[tokio::test]
#[ignore]
async fn test_r2_8_reranker_queryable_by_span_type() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "reranker.query".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("openinference.span.kind", "RERANKER")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let workspace_id = env.client.workspace_id();
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&span_type=RERANKER"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(
        !items.is_empty(),
        "?span_type=RERANKER must return at least one span"
    );
    for item in items {
        assert_eq!(item["span_type"], "RERANKER");
    }
    println!("✅ AC2.8: ?span_type=RERANKER returns RERANKER spans");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.9 — GET /api/v1/analytics/guardrails
// ---------------------------------------------------------------------------

/// AC2.9: GET /api/v1/analytics/guardrails returns valid JSON with the documented
/// shape AND correct aggregates. Hardened per P2-G4 to assert behavior, not just shape.
#[tokio::test]
#[ignore]
async fn test_r2_9_guardrails_analytics_endpoint_shape() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    // Use a unique rail.name so we can find our ingested rail in the
    // top_halting_rails list independently of any other test data.
    let unique_rail = format!("rail-{}", hex::encode(TestDataGenerator::span_id()));

    // Ingest a guardrails.request span (counts toward total_requests) and a
    // halted guardrails.rail span (counts toward halted_requests).
    let trace_id_req = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id_req,
            vec![SpanDefExt {
                name: "guardrails.request".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![kv_str("rail.type", "input")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_rail = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id_rail,
            vec![SpanDefExt {
                name: "guardrails.rail".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![
                    kv_str("rail.type", "input"),
                    kv_str("rail.name", &unique_rail),
                    kv_bool("rail.stop", true),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let workspace_id = env.client.workspace_id();
    let path = format!("/api/v1/analytics/guardrails?workspace_id={workspace_id}");
    let mut data = None;
    // 50 × 200ms = 10s: WAL-async ingest + analytics aggregation under the
    // full parallel suite occasionally needs more than a 5s window.
    for _ in 0..50 {
        let resp = env.client.get(&path).await?;
        assert!(
            resp.status().is_success(),
            "GET /api/v1/analytics/guardrails must return 2xx"
        );
        let body: serde_json::Value = resp.json().await?;
        // Poll until the halted rail is fully aggregated across *all* of the
        // endpoint's sub-queries — `halted_requests` AND its entry in
        // `top_halting_rails` (with halts ≥ 1). WAL-async ingest means these
        // separate aggregations can briefly lag each other while data is still
        // flushing, so waiting on the exact conditions the assertions below
        // check makes the test robust under the parallel suite (a weaker
        // "rail name present" predicate let a halts=0 snapshot through).
        let halted_requests = body["halted_requests"].as_i64().unwrap_or(0);
        let our_rail_halted = body["top_halting_rails"].as_array().is_some_and(|rails| {
            rails.iter().any(|r| {
                r["rail_name"].as_str() == Some(&unique_rail)
                    && r["halts"].as_i64().unwrap_or(0) >= 1
            })
        });
        if halted_requests >= 1 && our_rail_halted {
            data = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let data = data.ok_or_else(|| {
        anyhow::anyhow!(
            "guardrails analytics did not include rail_name={} within timeout",
            unique_rail
        )
    })?;

    // ---- Shape ----
    let total_requests = data["total_requests"]
        .as_i64()
        .expect("total_requests must be a number");
    let halted_requests = data["halted_requests"]
        .as_i64()
        .expect("halted_requests must be a number");
    let halt_rate = data["halt_rate"]
        .as_f64()
        .expect("halt_rate must be a number");
    let by_rail_type = data["by_rail_type"]
        .as_array()
        .expect("by_rail_type must be an array");
    let top_halting_rails = data["top_halting_rails"]
        .as_array()
        .expect("top_halting_rails must be an array");

    // ---- Behavior assertions (P2-G4) ----
    assert!(
        total_requests >= 1,
        "total_requests must be ≥ 1 after ingesting one guardrails.request span; got {}",
        total_requests
    );
    assert!(
        halted_requests >= 1,
        "halted_requests must be ≥ 1 after ingesting a halted rail span; got {}",
        halted_requests
    );
    assert!(
        (0.0..=1.0).contains(&halt_rate),
        "halt_rate must be in [0, 1]; got {}",
        halt_rate
    );

    // by_rail_type must contain an entry for rail_type=input with halted ≥ 1
    let input_breakdown = by_rail_type
        .iter()
        .find(|entry| entry["rail_type"].as_str() == Some("input"))
        .expect("by_rail_type must contain rail_type=input");
    assert!(
        input_breakdown["halted"].as_i64().unwrap_or(0) >= 1,
        "input rail must have halted ≥ 1; got {:?}",
        input_breakdown
    );

    // top_halting_rails must contain our unique rail with halts ≥ 1
    let our_rail = top_halting_rails
        .iter()
        .find(|entry| entry["rail_name"].as_str() == Some(&unique_rail))
        .unwrap_or_else(|| panic!("top_halting_rails must contain rail_name={}", unique_rail));
    assert!(
        our_rail["halts"].as_i64().unwrap_or(0) >= 1,
        "our rail must report halts ≥ 1; got {:?}",
        our_rail
    );
    assert_eq!(
        our_rail["rail_type"], "input",
        "our rail's rail_type must be 'input'"
    );

    println!("✅ AC2.9: guardrails analytics returns correct aggregates, not just shape");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC2.10 — NAT workflow appears in /api/v1/analytics/agents
// ---------------------------------------------------------------------------

/// AC2.10: NAT workflow appears in GET /api/v1/analytics/agents without code changes.
#[tokio::test]
#[ignore]
async fn test_r2_10_nat_agents_analytics_smoke() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let agent_name = format!("nat-agent-{}", hex::encode(TestDataGenerator::span_id()));

    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "nat.agent.step".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![kv_str("nat.function.name", &agent_name)],
                status_code: None,
            }],
        ))
        .await?;

    let workspace_id = env.client.workspace_id();
    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let path = format!("/api/v1/analytics/agents?workspace_id={workspace_id}");
    let data = poll_until(
        || {
            let client = &env.client;
            let path = path.clone();
            let agent_name = agent_name.clone();
            async move {
                let resp = client.get(&path).await?;
                assert!(
                    resp.status().is_success(),
                    "GET /api/v1/analytics/agents must return 2xx"
                );
                let data: serde_json::Value = resp.json().await?;
                let agents = data.as_array().expect("agents endpoint returns array");
                if agents
                    .iter()
                    .any(|a| a["agent_name"].as_str() == Some(&agent_name))
                {
                    Ok(Some(data))
                } else {
                    Ok(None)
                }
            }
        },
        Duration::from_secs(5),
        Duration::from_millis(200),
    )
    .await?;
    let agents = data.as_array().expect("agents endpoint returns array");
    let found = agents
        .iter()
        .any(|a| a["agent_name"].as_str() == Some(&agent_name));
    assert!(
        found,
        "NAT workflow agent '{}' must appear in /api/v1/analytics/agents",
        agent_name
    );
    println!("✅ AC2.10: NAT workflow agent appears in /api/v1/analytics/agents");
    Ok(())
}
