//! Phase 4 NeMo polish — end-to-end functional tests.
//!
//! Each test maps to an acceptance criterion from TECH-SPEC-PHASE-4.md §10.
//! All tests are `#[ignore]`; they run via `make functional_tests_fast`.

#[allow(unused_imports)]
use crate::*;
use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

// ===========================================================================
// AC4.1 — span links preserved as JSON
// ===========================================================================

/// AC4.1: a span emitted with an OTLP `links` array of 3 entries must surface
/// those links in the JSON `links` column on the response, with trace_id and
/// span_id hex-encoded and link attributes preserved verbatim.
#[tokio::test]
#[ignore]
async fn test_ac4_1_span_links_preserved() -> Result<()> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::span::Link;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};

    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let linked_trace_a = [0xAAu8; 16];
    let linked_span_a = [0x11u8; 8];
    let linked_trace_b = [0xBBu8; 16];
    let linked_span_b = [0x22u8; 8];
    let linked_trace_c = [0xCCu8; 16];
    let linked_span_c = [0x33u8; 8];

    let links = vec![
        Link {
            trace_id: linked_trace_a.to_vec(),
            span_id: linked_span_a.to_vec(),
            trace_state: String::new(),
            attributes: vec![KeyValue {
                key: "reason".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(AnyValue::StringValue("retry".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
        },
        Link {
            trace_id: linked_trace_b.to_vec(),
            span_id: linked_span_b.to_vec(),
            trace_state: "vendor=opaque".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        },
        Link {
            trace_id: linked_trace_c.to_vec(),
            span_id: linked_span_c.to_vec(),
            trace_state: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
        },
    ];

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
                name: "agent.retry".to_string(),
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 1_000_000,
                links,
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

    let links_json = &span["links"];
    let arr = links_json
        .as_array()
        .unwrap_or_else(|| panic!("links must be a JSON array, got: {links_json}"));
    assert_eq!(arr.len(), 3, "all 3 links must round-trip");

    // First link: trace_id_a, span_id_a, with reason=retry attribute.
    assert_eq!(arr[0]["trace_id"], "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(arr[0]["span_id"], "1111111111111111");
    assert_eq!(arr[0]["attributes"]["reason"], "retry");
    assert!(
        arr[0].get("trace_state").is_none(),
        "empty trace_state must be omitted"
    );

    // Second link: trace_state propagates; no attributes.
    assert_eq!(arr[1]["trace_id"], "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    assert_eq!(arr[1]["span_id"], "2222222222222222");
    assert_eq!(arr[1]["trace_state"], "vendor=opaque");
    assert!(
        arr[1].get("attributes").is_none(),
        "empty attributes must be omitted"
    );

    // Third link: minimal — just IDs.
    assert_eq!(arr[2]["trace_id"], "cccccccccccccccccccccccccccccccc");
    assert_eq!(arr[2]["span_id"], "3333333333333333");

    println!("✅ AC4.1: span links preserved verbatim into JSON column");
    Ok(())
}

/// AC4.1 negative-path: a span with no links surfaces an empty array — not
/// AC4.1 neg / PR8 follow-up: a span with no links must surface as `null` on
/// the wire (matching the `model_parameters` precedent), NOT as the literal
/// empty array. The storage column stays non-null `"[]"`; the workspaceion
/// layer (`parse_json_array`) maps the default to `None` so consumers can
/// treat "no data" uniformly across links / events / model_parameters.
#[tokio::test]
#[ignore]
async fn test_ac4_1_span_without_links_returns_null() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "no.links".to_string(),
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
        span["links"].is_null(),
        "no-links span must surface links as null, got: {}",
        span["links"]
    );
    assert!(
        span["events"].is_null(),
        "no-events span must surface events as null, got: {}",
        span["events"]
    );

    println!("✅ AC4.1 neg: no-links / no-events span surfaces null");
    Ok(())
}

// ===========================================================================
// AC4.2 — parent_span_id zero-hex normalized to root
// ===========================================================================

/// AC4.2: a span sent with `parent_span_id = [0u8; 8]` (some collectors do
/// this instead of empty bytes) must be recognized as a root span — the
/// trace-detail response surfaces no parent_span_id (or empty).
#[tokio::test]
#[ignore]
async fn test_ac4_2_parent_span_id_zero_hex_treated_as_root() -> Result<()> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};

    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Build a raw OTLP request with parent_span_id explicitly set to 8 zero
    // bytes (not empty). The converter must collapse this to "" so root-span
    // queries match.
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
                parent_span_id: vec![0u8; 8], // ← the wire-shape we're testing
                name: "root.span".to_string(),
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 1_000_000,
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

    // parent_span_id must surface as null / absent (Option::None) since the
    // helper normalized "[0u8; 8]" → "" → non_empty_string filters to None.
    let parent = &span["parent_span_id"];
    assert!(
        parent.is_null() || parent.as_str().map(|s| s.is_empty()).unwrap_or(false),
        "span with all-zero parent_span_id must surface as root (null/empty), got: {parent}"
    );

    println!("✅ AC4.2: 8-zero-bytes parent_span_id normalized to root");
    Ok(())
}

/// AC4.2 positive-control: a span with a real parent_span_id must NOT be
/// flattened to root.
#[tokio::test]
#[ignore]
async fn test_ac4_2_real_parent_span_id_preserved() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let parent_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![
                SpanDefExt {
                    name: "parent.span".to_string(),
                    span_id: parent_span_id,
                    parent_span_id: None,
                    attributes: vec![],
                    status_code: None,
                },
                SpanDefExt {
                    name: "child.span".to_string(),
                    span_id: child_span_id,
                    parent_span_id: Some(parent_span_id),
                    attributes: vec![],
                    status_code: None,
                },
            ],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace["spans"].as_array().expect("spans array");
    let parent_hex = hex::encode(parent_span_id);

    // Find the child and verify its parent_span_id matches the real parent's hex.
    let child = spans
        .iter()
        .find(|s| s["operation_name"] == "child.span")
        .or_else(|| spans.iter().find(|s| s["span_name"] == "child.span"))
        .expect("child span must be returned");
    assert_eq!(
        child["parent_span_id"].as_str().unwrap_or(""),
        parent_hex,
        "real parent_span_id must round-trip as hex, not get normalized to root"
    );

    println!("✅ AC4.2 ctrl: real parent_span_id preserved (not flattened)");
    Ok(())
}

// ===========================================================================
// AC4.3 — llm.cache.hit populates llm_cache_hit
// ===========================================================================

fn kv_str(key: &str, val: &str) -> (String, OtlpAnyValue) {
    (
        key.to_string(),
        OtlpAnyValue {
            value: Some(AnyValue::StringValue(val.to_string())),
        },
    )
}

fn kv_bool(key: &str, val: bool) -> (String, OtlpAnyValue) {
    (
        key.to_string(),
        OtlpAnyValue {
            value: Some(AnyValue::BoolValue(val)),
        },
    )
}

/// AC4.3: `llm.cache.hit=true` on a Guardrails LLM child must surface as
/// `llm_cache_hit: true` on the SpanDetail.
#[tokio::test]
#[ignore]
async fn test_ac4_3_llm_cache_hit_true_populates_field() -> Result<()> {
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
                attributes: vec![kv_bool("llm.cache.hit", true)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["llm_cache_hit"], true,
        "llm.cache.hit=true must surface as llm_cache_hit: true"
    );

    println!("✅ AC4.3: llm.cache.hit=true → llm_cache_hit=true");
    Ok(())
}

/// AC4.3 negative-path: when `llm.cache.hit` is absent, the column must
/// surface as null — distinct from an explicit `false` (miss). The tri-state
/// SMALLINT stores `-1` (unknown) for absent, which the API maps to null.
#[tokio::test]
#[ignore]
async fn test_ac4_3_llm_cache_hit_absent_returns_null() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "no.cache.attr".to_string(),
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

    let cache_hit = &span["llm_cache_hit"];
    assert!(
        cache_hit.is_null(),
        "absent llm.cache.hit must surface as null (unknown), got: {cache_hit}"
    );

    println!("✅ AC4.3 neg: absent llm.cache.hit surfaces as null");
    Ok(())
}

/// AC4.3 explicit-miss: `llm.cache.hit=false` must surface as `false`
/// (NOT null) so cache-hit-rate analytics can count explicit misses. This is
/// the case the old non-zero workspaceion collapsed into null.
#[tokio::test]
#[ignore]
async fn test_ac4_3_llm_cache_hit_false_populates_field() -> Result<()> {
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
                attributes: vec![kv_bool("llm.cache.hit", false)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["llm_cache_hit"], false,
        "explicit llm.cache.hit=false must surface as false, not null"
    );

    println!("✅ AC4.3: explicit llm.cache.hit=false → llm_cache_hit=false");
    Ok(())
}

// ===========================================================================
// AC4.4 — gen_ai.response.id populates llm_response_id
// ===========================================================================

/// AC4.4: `gen_ai.response.id=chatcmpl-abc123` populates `llm_response_id`.
#[tokio::test]
#[ignore]
async fn test_ac4_4_gen_ai_response_id_populates_field() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let unique_id = format!("chatcmpl-{}", hex::encode(TestDataGenerator::span_id()));

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.call".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![kv_str("gen_ai.response.id", &unique_id)],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    assert_eq!(
        span["llm_response_id"].as_str().unwrap_or(""),
        unique_id,
        "gen_ai.response.id must round-trip into llm_response_id"
    );

    println!("✅ AC4.4: gen_ai.response.id → llm_response_id");
    Ok(())
}

// ===========================================================================
// AC4.5 / AC4.6 — deployment.environment lands on every span; ?environment filter
// ===========================================================================

/// AC4.5: a resource attribute `deployment.environment=production` lands as
/// Span.environment on EVERY span in the request.
#[tokio::test]
#[ignore]
async fn test_ac4_5_deployment_environment_propagates_to_all_spans() -> Result<()> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};

    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Hand-build a resource with deployment.environment=production attached to
    // three spans in the same ResourceSpans batch.
    let spans: Vec<OtlpSpan> = (0..3)
        .map(|i| OtlpSpan {
            trace_id: trace_id.to_vec(),
            span_id: TestDataGenerator::span_id().to_vec(),
            name: format!("op.{i}"),
            start_time_unix_nano: now_ns + i,
            end_time_unix_nano: now_ns + i + 1_000_000,
            ..Default::default()
        })
        .collect();

    let resource_spans = ResourceSpans {
        resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue(service.clone())),
                    }),
                },
                KeyValue {
                    key: "deployment.environment".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue("production".to_string())),
                    }),
                },
            ],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans,
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
    let span_arr = trace["spans"].as_array().expect("spans array");

    assert_eq!(
        span_arr.len(),
        3,
        "all 3 spans must arrive (got {})",
        span_arr.len()
    );
    for span in span_arr {
        assert_eq!(
            span["environment"].as_str().unwrap_or(""),
            "production",
            "every span in the request must carry deployment.environment"
        );
    }

    println!("✅ AC4.5: deployment.environment propagated to every span in the resource");
    Ok(())
}

/// AC4.6: `?environment=production` filter returns only spans tagged
/// production; staging spans never leak in.
#[tokio::test]
#[ignore]
async fn test_ac4_6_environment_filter_returns_only_matching() -> Result<()> {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};

    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Ingest 2 separate ResourceSpans batches, one for prod, one for staging.
    let make_resource_spans = |env_name: &str, trace_id: &[u8; 16]| ResourceSpans {
        resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue(service.clone())),
                    }),
                },
                KeyValue {
                    key: "deployment.environment".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue(env_name.to_string())),
                    }),
                },
            ],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans: vec![OtlpSpan {
                trace_id: trace_id.to_vec(),
                span_id: TestDataGenerator::span_id().to_vec(),
                name: format!("op.{env_name}"),
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 1_000_000,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let prod_trace = TestDataGenerator::trace_id();
    let staging_trace = TestDataGenerator::trace_id();
    let req = opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![
            make_resource_spans("production", &prod_trace),
            make_resource_spans("staging", &staging_trace),
        ],
    };
    env.otlp.export_traces(req).await?;

    wait_for_trace_default(&env.client, &hex::encode(prod_trace)).await?;
    wait_for_trace_default(&env.client, &hex::encode(staging_trace)).await?;

    let workspace_id = env.client.workspace_id();

    // Positive: ?environment=production must return ≥ 1 row and never any staging row.
    let resp = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&environment=production"
        ))
        .await?;
    assert!(resp.status().is_success());
    let data: serde_json::Value = resp.json().await?;
    let items = data["items"].as_array().expect("items array");
    assert!(
        !items.is_empty(),
        "?environment=production must return ≥ 1 row"
    );
    for item in items {
        assert_eq!(
            item["environment"], "production",
            "?environment=production must not leak staging or other envs"
        );
    }

    // Negative: ?environment=does_not_exist returns 0 rows.
    let resp_none = env
        .client
        .get(&format!(
            "/api/v1/spans?workspace_id={workspace_id}&environment=does_not_exist_zzz"
        ))
        .await?;
    let data_none: serde_json::Value = resp_none.json().await?;
    assert_eq!(
        data_none["total"].as_i64().unwrap_or(-1),
        0,
        "unmatched environment must return total=0"
    );

    println!("✅ AC4.6: ?environment filter is live on /api/v1/spans");
    Ok(())
}

// ===========================================================================
// AC4.7 — gen_ai.request.* sampling params populate model_parameters JSON
// ===========================================================================

fn kv_double(key: &str, val: f64) -> (String, OtlpAnyValue) {
    (
        key.to_string(),
        OtlpAnyValue {
            value: Some(AnyValue::DoubleValue(val)),
        },
    )
}

fn kv_int(key: &str, val: i64) -> (String, OtlpAnyValue) {
    (
        key.to_string(),
        OtlpAnyValue {
            value: Some(AnyValue::IntValue(val)),
        },
    )
}

/// AC4.7: a span carrying gen_ai.request.temperature=0.7 and max_tokens=500
/// must produce a model_parameters object containing both fields.
#[tokio::test]
#[ignore]
async fn test_ac4_7_sampling_params_populate_model_parameters() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "llm.generate".to_string(),
                span_id,
                parent_span_id: None,
                attributes: vec![
                    kv_double("gen_ai.request.temperature", 0.7),
                    kv_int("gen_ai.request.max_tokens", 500),
                    kv_double("gen_ai.request.top_p", 0.95),
                ],
                status_code: None,
            }],
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let span = &trace["spans"][0];

    let params = &span["model_parameters"];
    assert!(
        params.is_object(),
        "model_parameters must be a JSON object when sampling keys present; got: {params}"
    );
    let temp = params["temperature"]
        .as_f64()
        .unwrap_or_else(|| panic!("temperature missing in model_parameters: {params}"));
    assert!(
        (temp - 0.7).abs() < 1e-9,
        "temperature must round-trip exactly: expected 0.7, got {temp}"
    );
    assert_eq!(
        params["max_tokens"], 500,
        "max_tokens must appear as integer 500"
    );
    let top_p = params["top_p"].as_f64().unwrap();
    assert!((top_p - 0.95).abs() < 1e-9, "top_p must round-trip");

    println!("✅ AC4.7: gen_ai.request.* sampling params surface in model_parameters JSON");
    Ok(())
}

/// AC4.7 negative-path: a span with NO sampling attributes must surface
/// model_parameters as null (not as the literal "{}" string).
#[tokio::test]
#[ignore]
async fn test_ac4_7_no_sampling_params_returns_null() -> Result<()> {
    let env = TestEnv::setup().await?;
    let service = TestDataGenerator::service_name();
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .export_traces(env.otlp.build_multi_span_trace_with_attributes(
            &service,
            &trace_id,
            vec![SpanDefExt {
                name: "no.sampling.attrs".to_string(),
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

    let params = &span["model_parameters"];
    assert!(
        params.is_null(),
        "no sampling attrs → model_parameters must be null; got: {params}"
    );

    println!("✅ AC4.7 neg: absent sampling attrs surface as null");
    Ok(())
}

// ===========================================================================
// AC4.11 — NEMO.md auth section published (doc review backed by file check)
// ===========================================================================

/// AC4.11: docs/INTEGRATIONS/NEMO.md must publish an Authentication section
/// with both the correct Bearer-prefixed example and the common-mistakes
/// callouts. This is a file-content check (not `#[ignore]`) so the doc
/// requirement is enforced on every CI run.
#[test]
fn test_ac4_11_nemo_md_publishes_authentication_section() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("docs/INTEGRATIONS/NEMO.md");
    let body =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {path:?}: {e}"));

    // Section header is present.
    assert!(
        body.contains("## Authentication"),
        "NEMO.md must contain a top-level Authentication section"
    );

    // Correct form is documented.
    assert!(
        body.contains("Bearer zk_live_") || body.contains("Bearer ${env:ZRADAR_API_KEY}"),
        "NEMO.md auth section must show the correct `Bearer <key>` form"
    );

    // Common mistakes are called out — at minimum the missing-prefix case.
    assert!(
        body.to_lowercase().contains("missing") && body.to_lowercase().contains("bearer"),
        "NEMO.md auth section must call out the missing-Bearer-prefix gotcha"
    );

    // Wrong scheme (`Token ...`) is called out.
    assert!(
        body.contains("Token"),
        "NEMO.md auth section must mention the wrong `Token ` scheme as ✗"
    );

    println!("✅ AC4.11: NEMO.md auth section published with right + wrong examples");
}
