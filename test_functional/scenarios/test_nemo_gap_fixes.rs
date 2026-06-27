//! Functional regression tests for gap fixes identified in the Phase 0-2 audit.
//!
//! All tests are `#[ignore]` and run only against a live stack: `make functional_tests`.

#[allow(unused_imports)]
use crate::*;
use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use prost::Message;

fn kv_str(key: &str, val: &str) -> (String, OtlpAnyValue) {
    (
        key.to_string(),
        OtlpAnyValue {
            value: Some(AnyValue::StringValue(val.to_string())),
        },
    )
}

fn otlp_http_url() -> String {
    std::env::var("TEST_OTLP_HTTP_URL").unwrap_or_else(|_| "http://localhost:4318".to_string())
}

fn test_auth_header(env: &TestEnv) -> String {
    format!("Bearer {}", env.api_key)
}

async fn disable_content_capture(env: &TestEnv) -> Result<()> {
    let project_id = env.client.project_id();
    let default_settings = ApiClient::get_json(
        env.client
            .get(&format!("/api/v1/projects/{project_id}/settings"))
            .await?,
    )
    .await?;
    let disable_body = serde_json::json!({
        "traces_retention_days": default_settings["traces_retention_days"].as_i64().unwrap_or(30),
        "metrics_retention_days": default_settings["metrics_retention_days"].as_i64().unwrap_or(30),
        "logs_retention_days": default_settings["logs_retention_days"].as_i64().unwrap_or(30),
        "max_ingestion_rate": default_settings["max_ingestion_rate"].clone(),
        "file_push_interval_secs": default_settings["file_push_interval_secs"].as_i64().unwrap_or(60),
        "blocked": false,
        "capture_llm_content_enabled": false,
    });
    let update_resp = env
        .client
        .put(
            &format!("/api/v1/projects/{project_id}/settings"),
            &disable_body,
        )
        .await?;
    assert!(
        update_resp.status().is_success(),
        "project settings update must succeed before content-capture assertion; status={}",
        update_resp.status()
    );
    Ok(())
}

fn assert_content_scrubbed(span: &serde_json::Value) {
    assert!(
        span["llm_input"].as_str().unwrap_or("").is_empty(),
        "llm_input must be empty when capture is disabled"
    );
    assert!(
        span["llm_output"].as_str().unwrap_or("").is_empty(),
        "llm_output must be empty when capture is disabled"
    );

    if let Some(attrs) = span["attributes"].as_object() {
        for key in attrs.keys() {
            assert!(
                !key.starts_with("gen_ai.content."),
                "gen_ai.content.* must be stripped from attributes when capture is disabled; found key: {key}"
            );
        }
    }

    if let Some(events) = span["events"].as_array() {
        for event in events {
            let name = event["name"].as_str().unwrap_or("");
            assert!(
                name != "gen_ai.content.prompt" && name != "gen_ai.content.completion",
                "gen_ai.content.* events must be stripped when capture is disabled; found: {name}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// G1: HTTP /v1/logs eval-score extraction
// ---------------------------------------------------------------------------

/// G1: Evaluation scores posted via HTTP /v1/logs are persisted.
///
/// NeMo Evaluator's canonical transport is HTTP; gRPC was already covered.
/// This test verifies the shared score_extractor is wired into the HTTP path.
#[tokio::test]
#[ignore]
async fn test_g1_http_logs_persists_eval_scores() -> Result<()> {
    let env = TestEnv::setup().await?;
    let otlp_http_url = otlp_http_url();

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let trace_id_hex = hex::encode(trace_id);
    let span_id_hex = hex::encode(span_id);

    // Build a log request with score.* attributes matching the extractor contract.
    let attrs: Vec<(&str, &str)> = vec![
        ("score.trace_id", &trace_id_hex),
        ("score.span_id", &span_id_hex),
        ("score.name", "accuracy"),
        ("score.value", "0.95"),
    ];
    let log_req = env.otlp.build_log_request_with_attrs(
        "eval-service",
        9,
        "score event",
        &trace_id,
        &span_id,
        &attrs,
    );

    let body = log_req.encode_to_vec();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/logs", otlp_http_url))
        .header("content-type", "application/x-protobuf")
        .header("authorization", test_auth_header(&env))
        .header("x-tenant-id", env.tenant_id.to_string())
        .header("x-project-id", env.project_id.to_string())
        .body(body)
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        200,
        "HTTP /v1/logs must return 200 when scores present"
    );

    let db = crate::helpers::DbClient::from_env().await?;
    let score_files = poll_until(
        || {
            let db = &db;
            let tenant_id = env.tenant_id;
            let project_id = env.project_id;
            async move {
                let rows = db
                    .file_list_entries(&tenant_id, &project_id, "scores")
                    .await?;
                if rows.iter().any(|row| row.records > 0 && !row.deleted) {
                    Ok(Some(rows))
                } else {
                    Ok(None)
                }
            }
        },
        Duration::from_secs(10),
        DEFAULT_POLL_INTERVAL,
    )
    .await?;
    assert!(
        score_files
            .iter()
            .any(|row| row.records > 0 && !row.deleted),
        "HTTP /v1/logs must persist at least one score file for tenant/project"
    );

    println!("✅ G1: HTTP /v1/logs persists eval scores with auth/context headers");
    Ok(())
}

// ---------------------------------------------------------------------------
// G4: llm_response_model filter on traces
// ---------------------------------------------------------------------------

/// G4: ?llm_response_model= filter on /api/v1/traces returns only matching traces.
#[tokio::test]
#[ignore]
async fn test_g4_trace_llm_response_model_filter() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let unique_model = format!("gpt-g4-{}", hex::encode(TestDataGenerator::span_id()));

    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![kv_str("gen_ai.response.model", &unique_model)],
                status_code: None,
            }],
        ))
        .await?;

    wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;

    let project_id = env.client.project_id();

    // Filter by the unique response model — must return at least one trace.
    let resp = env
        .client
        .get(&format!(
            "/api/v1/traces?project_id={project_id}&llm_response_model={unique_model}"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let total = data["total"].as_i64().unwrap_or(0);
    assert!(
        total >= 1,
        "?llm_response_model filter on traces must return ≥ 1 trace (got {})",
        total
    );

    // A different model must return 0 for this unique value.
    let resp_none = env
        .client
        .get(&format!(
            "/api/v1/traces?project_id={project_id}&llm_response_model=nonexistent_model_zzz"
        ))
        .await?;
    assert!(resp_none.status().is_success());
    let data_none: serde_json::Value = resp_none.json().await?;
    assert_eq!(
        data_none["total"].as_i64().unwrap_or(-1),
        0,
        "?llm_response_model with no match must return total=0"
    );

    println!("✅ G4: ?llm_response_model filter is live on /api/v1/traces");
    Ok(())
}

// ---------------------------------------------------------------------------
// G7: OQ18 — exception survives 200 LLM_NEW_TOKEN events
// ---------------------------------------------------------------------------

/// G7: When a span carries 200 LLM_NEW_TOKEN events before an exception,
/// the exception must appear in the span's events JSON.
/// Verifies that noise is dropped BEFORE the count cap is applied.
#[tokio::test]
#[ignore]
async fn test_g7_oq18_exception_survives_new_token_flood() -> Result<()> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};

    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    // Build 200 LLM_NEW_TOKEN events then one exception.
    let mut events: Vec<Event> = (0..200)
        .map(|_| Event {
            name: "token.event".to_string(),
            attributes: vec![KeyValue {
                key: "nat.event_type".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(AnyValue::StringValue("LLM_NEW_TOKEN".to_string())),
                }),
            }],
            ..Default::default()
        })
        .collect();
    events.push(Event {
        name: "exception".to_string(),
        attributes: vec![KeyValue {
            key: "exception.message".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::StringValue("real error".to_string())),
            }),
        }],
        ..Default::default()
    });

    // Build a raw ResourceSpans with the events attached to the span.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let resource_spans = ResourceSpans {
        resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(AnyValue::StringValue(service.clone())),
                }),
            }],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans: vec![OtlpSpan {
                trace_id: trace_id.to_vec(),
                span_id: span_id.to_vec(),
                name: "llm.stream".to_string(),
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 1_000_000,
                events,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let request = opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    };

    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    // The events column should contain the exception but not the token events.
    let events_json = span["events"].clone();
    let events_arr = events_json.as_array().expect("events must be a JSON array");

    assert!(
        events_arr.iter().any(|e| e["name"] == "exception"),
        "exception must survive after 200 LLM_NEW_TOKEN events (OQ18); got events: {events_json}"
    );
    assert!(
        events_arr.iter().all(|e| e["name"] != "token.event"),
        "LLM_NEW_TOKEN events must not appear in stored events JSON"
    );

    println!("✅ G7: exception survives 200 LLM_NEW_TOKEN events (OQ18 cap order correct)");
    Ok(())
}

// ---------------------------------------------------------------------------
// G9: start-only time filter
// ---------------------------------------------------------------------------

/// G9: ?start_time= without ?end_time= must not silently drop the time filter.
#[tokio::test]
#[ignore]
async fn test_g9_start_time_only_filter_applied() -> Result<()> {
    let env = TestEnv::setup().await?;
    let project_id = env.client.project_id();
    let service = TestDataGenerator::service_name();

    // Ingest a span so there's data.
    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "test.span".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![],
                status_code: None,
            }],
        ))
        .await?;
    wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;

    // Query with a future start_time — should return 0 results (filter is active).
    let future_start = "2099-01-01T00:00:00Z";
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?project_id={project_id}&start_time={future_start}"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let total = data["total"].as_i64().unwrap_or(-1);
    assert_eq!(
        total, 0,
        "start_time filter must be applied even without end_time; \
        a future start must return 0 results (got {})",
        total
    );

    println!("✅ G9: start_time-only filter is applied correctly");
    Ok(())
}

// ---------------------------------------------------------------------------
// G3/G6: content-capture scrubs events and attributes
// ---------------------------------------------------------------------------

/// G3/G6: When content capture is disabled for a project, the ingested span
/// must not contain prompt/completion text in llm_input, llm_output, events,
/// or attributes columns.
///
/// Note: this test only fires if the project has capture disabled in its
/// settings. The test updates the real project settings row, ingests a span,
/// and verifies prompt/completion text is scrubbed from all response columns.
#[tokio::test]
#[ignore]
async fn test_g3_g6_content_capture_disabled_scrubs_all_columns() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();

    disable_content_capture(&env).await?;

    // Ingest a span with LLM content in both attributes and events.
    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id: TestDataGenerator::span_id(),
                parent_span_id: None,
                attributes: vec![
                    kv_str("gen_ai.content.prompt", "secret prompt text"),
                    kv_str("gen_ai.content.completion", "secret completion text"),
                    kv_str("model.id", "gpt-4"),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace = wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;
    let span = &trace["spans"][0];

    assert_content_scrubbed(span);

    println!("✅ G3/G6: content-capture disabled scrubs llm_input, llm_output, events, attributes");
    Ok(())
}

/// G3/G6: OTLP/HTTP traces must use the same project settings-backed content
/// capture policy as gRPC traces.
#[tokio::test]
#[ignore]
async fn test_g3_g6_http_traces_content_capture_disabled_scrubs_all_columns() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();

    disable_content_capture(&env).await?;

    let trace_id = TestDataGenerator::trace_id();
    let req = env.otlp.build_multi_span_trace_with_attributes(
        &service,
        &trace_id,
        vec![SpanDefExt {
            name: "llm.http.call".to_string(),
            span_id: TestDataGenerator::span_id(),
            parent_span_id: None,
            attributes: vec![
                kv_str("gen_ai.content.prompt", "http secret prompt"),
                kv_str("gen_ai.content.completion", "http secret completion"),
                kv_str("model.id", "gpt-4"),
            ],
            status_code: None,
        }],
    );

    let resp = reqwest::Client::new()
        .post(format!("{}/v1/traces", otlp_http_url()))
        .header("content-type", "application/x-protobuf")
        .header("authorization", test_auth_header(&env))
        .header("x-tenant-id", env.tenant_id.to_string())
        .header("x-project-id", env.project_id.to_string())
        .body(req.encode_to_vec())
        .send()
        .await?;

    assert_eq!(
        resp.status(),
        200,
        "OTLP/HTTP /v1/traces must accept protobuf traces when content capture is disabled"
    );

    let trace = wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;
    assert_content_scrubbed(&trace["spans"][0]);

    println!("✅ G3/G6: OTLP/HTTP traces honor disabled content-capture settings");
    Ok(())
}

/// HTTP guard-chain parity: OTLP/HTTP traces must enforce the same
/// project-level ingestion rate limits as gRPC traces.
#[tokio::test]
#[ignore]
async fn test_http_trace_project_ingestion_rate_limited() -> Result<()> {
    let env = TestEnv::setup().await?;
    let project_id = env.client.project_id();
    let service = TestDataGenerator::service_name();

    let settings_resp = env
        .client
        .put(
            &format!("/api/v1/projects/{project_id}/settings"),
            &serde_json::json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": 0,
                "file_push_interval_secs": 300,
                "blocked": false,
                "capture_llm_content_enabled": true
            }),
        )
        .await?;
    assert_eq!(settings_resp.status(), 200);

    let trace_id = TestDataGenerator::trace_id();
    let req = env.otlp.build_multi_span_trace_with_attributes(
        &service,
        &trace_id,
        vec![SpanDefExt {
            name: "http.rate_limited".to_string(),
            span_id: TestDataGenerator::span_id(),
            parent_span_id: None,
            attributes: vec![],
            status_code: None,
        }],
    );

    let resp = reqwest::Client::new()
        .post(format!("{}/v1/traces", otlp_http_url()))
        .header("content-type", "application/x-protobuf")
        .header("authorization", test_auth_header(&env))
        .header("x-tenant-id", env.tenant_id.to_string())
        .header("x-project-id", env.project_id.to_string())
        .body(req.encode_to_vec())
        .send()
        .await?;

    assert_eq!(
        resp.status(),
        429,
        "OTLP/HTTP traces must enforce project max_ingestion_rate"
    );

    println!("✅ HTTP guard-chain parity: trace ingestion rate limits apply to OTLP/HTTP");
    Ok(())
}
