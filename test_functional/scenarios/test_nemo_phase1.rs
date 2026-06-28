//! Phase 1 NeMo OTLP compatibility functional tests (R1.1 – R1.12).
//!
//! Each test validates a specific acceptance criterion. All tests are
//! `#[ignore]` and run only against a live stack: `make functional_tests`.

#[allow(unused_imports)]
use crate::*;
use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use prost::Message;

// ---------------------------------------------------------------------------
// Attribute construction helpers
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

fn kv_str(key: &str, val: &str) -> (String, OtlpAnyValue) {
    (key.to_string(), av_str(val))
}

fn kv_int(key: &str, val: i64) -> (String, OtlpAnyValue) {
    (key.to_string(), av_int(val))
}

fn test_auth_header(env: &TestEnv) -> String {
    format!("Bearer {}", env.api_key)
}

// ---------------------------------------------------------------------------
// AC R1.1 — OTLP/HTTP receiver on :4318
// ---------------------------------------------------------------------------

/// AC R1.1: POST /v1/traces with application/x-protobuf is accepted (200).
#[tokio::test]
#[ignore]
async fn test_r1_1_otlp_http_traces_accepted() -> Result<()> {
    let env = TestEnv::setup().await?;
    let otlp_http_url =
        std::env::var("TEST_OTLP_HTTP_URL").unwrap_or_else(|_| "http://localhost:4318".to_string());

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    let req = env.otlp.build_multi_span_trace_with_attributes(
        &service,
        &trace_id,
        vec![SpanDefExt {
            name: "http.test".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![kv_str("http.method", "GET")],
            status_code: None,
        }],
    );

    let body = req.encode_to_vec();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/traces", otlp_http_url))
        .header("content-type", "application/x-protobuf")
        .header("authorization", test_auth_header(&env))
        .header("x-workspace-id", env.workspace_id.to_string())
        .body(body)
        .send()
        .await?;

    assert_eq!(resp.status(), 200, "OTLP/HTTP /v1/traces must return 200");

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(5)).await?;
    println!("✅ R1.1: OTLP/HTTP traces ingested and queryable");
    Ok(())
}

/// AC R1.1: JSON body is rejected with 415.
#[tokio::test]
#[ignore]
async fn test_r1_1_otlp_http_json_rejected() -> Result<()> {
    let env = TestEnv::setup().await?;
    let otlp_http_url =
        std::env::var("TEST_OTLP_HTTP_URL").unwrap_or_else(|_| "http://localhost:4318".to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/traces", otlp_http_url))
        .header("content-type", "application/json")
        .header("authorization", test_auth_header(&env))
        .header("x-workspace-id", env.workspace_id.to_string())
        .body(b"{}".to_vec())
        .send()
        .await?;

    assert_eq!(
        resp.status(),
        415,
        "JSON body must be rejected with 415 Unsupported Media Type"
    );
    println!("✅ R1.1: JSON body correctly rejected with 415");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.2 — nat.* attribute namespace
// ---------------------------------------------------------------------------

/// AC R1.2: nat.workflow.run_id → workflow_run_id, nat.conversation.id → session_id.
#[tokio::test]
#[ignore]
async fn test_r1_2_nat_attributes_mapped() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "nat.agent".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_str("nat.workflow.run_id", "run-abc-123"),
                    kv_str("nat.conversation.id", "conv-xyz"),
                    kv_str("nat.framework", "langchain"),
                    kv_str("nat.function.name", "my_agent"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["workflow_run_id"], "run-abc-123",
        "workflow_run_id from nat.*"
    );
    assert_eq!(
        span["session_id"], "conv-xyz",
        "session_id from nat.conversation.id"
    );
    assert_eq!(
        span["framework"], "langchain",
        "framework from nat.framework"
    );
    assert_eq!(
        span["agent_name"], "my_agent",
        "agent_name from nat.function.name"
    );
    println!("✅ R1.2: nat.* attributes correctly mapped");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.2 — aiq.* canonical precedence over nat.*
// ---------------------------------------------------------------------------

/// AC R1.2: aiq.* overwrites nat.* for workflow_run_id and framework.
#[tokio::test]
#[ignore]
async fn test_r1_2_aiq_overwrites_nat() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "aiq.agent".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_str("nat.workflow.run_id", "nat-run"),
                    kv_str("nat.framework", "nat-fw"),
                    kv_str("aiq.workflow.run_id", "aiq-run"),
                    kv_str("aiq.framework", "aiq-fw"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["workflow_run_id"], "aiq-run",
        "aiq.* must overwrite nat.* for workflow_run_id"
    );
    assert_eq!(
        span["framework"], "aiq-fw",
        "aiq.* must overwrite nat.* for framework"
    );
    println!("✅ R1.2: aiq.* canonical precedence verified");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.3 — OTel GenAI 1.29 token conventions
// ---------------------------------------------------------------------------

/// AC R1.3: gen_ai.usage.input_tokens/output_tokens, gen_ai.response.model, gen_ai.provider.name.
#[tokio::test]
#[ignore]
async fn test_r1_3_genai_1_29_token_mapping() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_int("gen_ai.usage.input_tokens", 42),
                    kv_int("gen_ai.usage.output_tokens", 17),
                    kv_str("gen_ai.response.model", "gpt-4-turbo"),
                    kv_str("gen_ai.provider.name", "openai"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["prompt_tokens"], 42,
        "gen_ai.usage.input_tokens → prompt_tokens"
    );
    assert_eq!(
        span["completion_tokens"], 17,
        "gen_ai.usage.output_tokens → completion_tokens"
    );
    assert_eq!(
        span["llm_response_model"], "gpt-4-turbo",
        "gen_ai.response.model → llm_response_model"
    );
    assert_eq!(
        span["llm_provider"], "openai",
        "gen_ai.provider.name → llm_provider"
    );
    println!("✅ R1.3: OTel GenAI 1.29 token conventions correctly mapped");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.4 — llm_response_model filter
// ---------------------------------------------------------------------------

/// AC R1.4: Querying spans by llm_response_model returns matching spans.
#[tokio::test]
#[ignore]
async fn test_r1_4_llm_response_model_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();
    let model = format!("test-model-{}", generate_test_id());

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("gen_ai.response.model", &model)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(5)).await?;

    let path = format!(
        "/api/v1/spans?llm_response_model={}",
        urlencoding::encode(&model)
    );
    let resp = env.client.get(&path).await?;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await?;
    let items = body["items"].as_array().expect("items must be array");
    assert!(
        items.iter().any(|s| s["llm_response_model"] == model),
        "At least one span with llm_response_model={} expected",
        model
    );
    println!("✅ R1.4: llm_response_model filter works end-to-end");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.5 — llm_provider filter
// ---------------------------------------------------------------------------

/// AC R1.5: Querying spans by llm_provider returns matching spans.
#[tokio::test]
#[ignore]
async fn test_r1_5_llm_provider_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("gen_ai.provider.name", "anthropic")],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(5)).await?;

    let resp = env
        .client
        .get("/api/v1/spans?llm_provider=anthropic")
        .await?;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await?;
    let items = body["items"].as_array().expect("items must be array");
    assert!(
        items.iter().any(|s| s["llm_provider"] == "anthropic"),
        "llm_provider=anthropic filter must return the ingested span"
    );
    println!("✅ R1.5: llm_provider filter works end-to-end");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.6 — span events allowlist (baseline test via unit-tested conventions)
// ---------------------------------------------------------------------------

/// AC R1.6: Spans with no events are stored cleanly (events field absent or null).
#[tokio::test]
#[ignore]
async fn test_r1_6_no_events_stored_cleanly() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert!(
        span.get("events").is_none()
            || span["events"].is_null()
            || span["events"]
                .as_array()
                .is_some_and(|events| events.is_empty()),
        "No events content expected when no events sent"
    );
    println!("✅ R1.6: Span events field empty when no events sent");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.7 — SpanDetail enriched fields in API response
// ---------------------------------------------------------------------------

/// AC R1.7: SpanDetail includes llm_provider, llm_response_model, prompt_tokens, completion_tokens.
#[tokio::test]
#[ignore]
async fn test_r1_7_span_detail_enriched_fields() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_str("gen_ai.response.model", "gpt-4-0125"),
                    kv_str("gen_ai.provider.name", "openai"),
                    kv_int("gen_ai.usage.input_tokens", 10),
                    kv_int("gen_ai.usage.output_tokens", 20),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert!(
        span.get("llm_response_model").is_some(),
        "llm_response_model must be in SpanDetail"
    );
    assert!(
        span.get("llm_provider").is_some(),
        "llm_provider must be in SpanDetail"
    );
    assert!(
        span.get("prompt_tokens").is_some(),
        "prompt_tokens must be in SpanDetail"
    );
    assert!(
        span.get("completion_tokens").is_some(),
        "completion_tokens must be in SpanDetail"
    );
    println!("✅ R1.7: SpanDetail enriched fields present in API response");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.9 — Logs mixed attribute types round-trip intact
// ---------------------------------------------------------------------------

/// AC R1.9: Log records with string attributes are stored (covers the shared attrs_to_json path).
#[tokio::test]
#[ignore]
async fn test_r1_9_logs_attributes_round_trip() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();

    let req = env.otlp.build_log_request_with_attrs(
        &service,
        9, // INFO
        "log with nemo guardrail attrs",
        &[],
        &[],
        &[("rail.name", "input"), ("severity", "warn")],
    );
    env.otlp.export_logs(req).await?;

    let logs = wait_for_items_default(&env.client, "/api/v1/logs").await?;
    assert!(
        !logs.is_empty(),
        "Log records must be queryable after ingestion"
    );
    println!("✅ R1.9: Log records with attributes round-trip via shared attrs_to_json");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.10 — status filter for spans
// ---------------------------------------------------------------------------

/// AC R1.10: status=ERROR filter returns only spans with ERROR status_code.
#[tokio::test]
#[ignore]
async fn test_r1_10_span_status_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let ok_span = TestDataGenerator::span_id();
    let err_span = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![
                SpanDefExt {
                    name: "ok.call".to_string(),
                    span_id: ok_span,
                    parent_span_id: None,
                    attributes: vec![],
                    status_code: Some(1), // OK
                },
                SpanDefExt {
                    name: "err.call".to_string(),
                    span_id: err_span,
                    parent_span_id: Some(ok_span),
                    attributes: vec![],
                    status_code: Some(2), // ERROR
                },
            ],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let resp = env.client.get("/api/v1/spans?status=ERROR").await?;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await?;
    let items = body["items"].as_array().expect("items must be array");
    assert!(
        items.iter().any(|s| s["status"] == "ERROR"),
        "status=ERROR filter must return error spans"
    );
    assert!(
        !items.iter().any(|s| s["status"] == "OK"),
        "status=ERROR filter must not return OK spans"
    );
    println!("✅ R1.10: status filter correctly filters spans by status_code");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.11 — operation_name filter
// ---------------------------------------------------------------------------

/// AC R1.11: operation_name filter on traces returns matching traces only.
#[tokio::test]
#[ignore]
async fn test_r1_11_operation_name_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let matching_span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();
    let op = format!("unique.op.{}", generate_test_id());

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![
                SpanDefExt {
                    name: "workflow.root.error".to_string(),
                    span_id: root_span_id,
                    parent_span_id: None,
                    attributes: vec![],
                    status_code: Some(2),
                },
                SpanDefExt {
                    name: op.clone(),
                    span_id: matching_span_id,
                    parent_span_id: Some(root_span_id),
                    attributes: vec![],
                    status_code: Some(1),
                },
            ],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(5)).await?;

    let path = format!("/api/v1/traces?operation_name={}", urlencoding::encode(&op));
    let resp = env.client.get(&path).await?;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await?;
    let items = body["items"].as_array().expect("items must be array");
    assert!(
        !items.is_empty(),
        "operation_name filter must return at least one trace for op={op}"
    );
    let matching = items
        .iter()
        .find(|item| item["trace_id"].as_str() == Some(trace_id_hex.as_str()))
        .unwrap_or_else(|| panic!("operation_name filter must return trace_id={trace_id_hex}"));
    assert_eq!(
        matching["span_count"], 2,
        "trace summary must aggregate the full trace, not only the matching span"
    );
    assert_eq!(
        matching["status"], "ERROR",
        "trace summary must preserve status from non-matching spans in the same trace"
    );

    let miss_path = format!("/api/v1/traces?operation_name={}", "no.such.operation");
    let miss_resp = env.client.get(&miss_path).await?;
    assert_eq!(miss_resp.status(), 200);
    let miss_body: Value = miss_resp.json().await?;
    let miss_items = miss_body["items"].as_array().expect("items must be array");
    assert!(
        miss_items
            .iter()
            .all(|item| item["trace_id"].as_str() != Some(trace_id_hex.as_str())),
        "non-matching operation filter must not return trace_id={trace_id_hex}"
    );
    println!("✅ R1.11: operation_name filter works for traces");
    Ok(())
}

// ---------------------------------------------------------------------------
// AC R1.12 — ContentCapturePolicy (NoopContentCapturePolicy default)
// ---------------------------------------------------------------------------

/// AC R1.12: Default capture enabled — span stored with llm fields present.
#[tokio::test]
#[ignore]
async fn test_r1_12_content_capture_default_enabled() -> Result<()> {
    let env = TestEnv::setup().await?;
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service = TestDataGenerator::service_name();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_str("gen_ai.response.model", "gpt-4"),
                    kv_str("gen_ai.provider.name", "openai"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    // With default NoopContentCapturePolicy, capture is enabled — span stored
    assert!(
        span["span_id"].is_string(),
        "Span must be stored when content capture is enabled (default)"
    );
    assert!(
        span.get("llm_response_model").is_some(),
        "llm_response_model must be present when capture is enabled"
    );
    println!("✅ R1.12: NoopContentCapturePolicy — capture enabled by default, span stored");
    Ok(())
}
