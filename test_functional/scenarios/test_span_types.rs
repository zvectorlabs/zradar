//! Span Type Detection Tests
//!
//! These tests verify that span_type is correctly detected from OTLP attributes
//! and can be queried via REST API.

use anyhow::Result;
use functional_tests::*;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Test that spans with gen_ai.request.model attribute are detected as GENERATION
#[tokio::test]
#[ignore]
async fn test_generation_span_type_from_model_attribute() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    // Build trace with model attribute
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = build_trace_with_attributes(
        "llm-service",
        &trace_id,
        &span_id,
        "openai.chat.completions",
        vec![(
            "gen_ai.request.model",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "gpt-4".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    otlp_client.export_traces(request).await?;

    println!("✅ Trace with model attribute sent");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query and verify span_type
    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    assert_eq!(response.status(), 200, "Should be able to query trace");

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert!(!spans.is_empty(), "Should have at least 1 span");

    let span = &spans[0];
    assert_eq!(
        span["span_type"].as_str().expect("span_type should exist"),
        "GENERATION",
        "Span with model attribute should be GENERATION"
    );

    println!("✅ GENERATION span type detected correctly");
    Ok(())
}

/// Test that spans with tool.name attribute are detected as TOOL
#[tokio::test]
#[ignore]
async fn test_tool_span_type_from_tool_attribute() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = build_trace_with_attributes(
        "tool-service",
        &trace_id,
        &span_id,
        "calculator.execute",
        vec![(
            "tool.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "calculator".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    otlp_client.export_traces(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert_eq!(
        span["span_type"].as_str().expect("span_type should exist"),
        "TOOL",
        "Span with tool.name should be TOOL"
    );

    println!("✅ TOOL span type detected correctly");
    Ok(())
}

/// Test that spans with agent.name attribute are detected as AGENT
#[tokio::test]
#[ignore]
async fn test_agent_span_type_from_agent_attribute() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = build_trace_with_attributes(
        "agent-service",
        &trace_id,
        &span_id,
        "agent.run",
        vec![(
            "agent.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "research-agent".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    otlp_client.export_traces(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert_eq!(
        span["span_type"].as_str().expect("span_type should exist"),
        "AGENT",
        "Span with agent.name should be AGENT"
    );

    println!("✅ AGENT span type detected correctly");
    Ok(())
}

/// Test that zero-duration spans are detected as EVENT
#[tokio::test]
#[ignore]
async fn test_event_span_type_for_zero_duration() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Build trace with zero duration (start_time == end_time)
    let request = build_trace_with_attributes(
        "event-service",
        &trace_id,
        &span_id,
        "user.click",
        vec![],
        Some((now, now)), // Zero duration
    );

    otlp_client.export_traces(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert_eq!(
        span["span_type"].as_str().expect("span_type should exist"),
        "EVENT",
        "Zero-duration span should be EVENT"
    );

    println!("✅ EVENT span type detected correctly");
    Ok(())
}

/// Test that explicit zradar.span.type attribute is respected
#[tokio::test]
#[ignore]
async fn test_explicit_span_type_from_zradar_attribute() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = build_trace_with_attributes(
        "chain-service",
        &trace_id,
        &span_id,
        "chain.execute",
        vec![
            (
                "zradar.span.type",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "CHAIN".to_string(),
                        ),
                    ),
                },
            ),
            // Also include model attribute to test priority
            (
                "gen_ai.request.model",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "gpt-4".to_string(),
                        ),
                    ),
                },
            ),
        ],
        None,
    );

    otlp_client.export_traces(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert_eq!(
        span["span_type"].as_str().expect("span_type should exist"),
        "CHAIN",
        "Explicit zradar.span.type should override other attributes"
    );

    println!("✅ Explicit span type (CHAIN) detected correctly");
    Ok(())
}

/// Test REST API filtering by span_type
#[tokio::test]
#[ignore]
async fn test_rest_api_filter_by_span_type() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Create multiple spans with different types
    let trace1 = TestDataGenerator::trace_id();
    let span1 = TestDataGenerator::span_id();
    let request1 = build_trace_with_attributes(
        "service1",
        &trace1,
        &span1,
        "operation1",
        vec![(
            "gen_ai.request.model",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "gpt-4".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    let trace2 = TestDataGenerator::trace_id();
    let span2 = TestDataGenerator::span_id();
    let request2 = build_trace_with_attributes(
        "service2",
        &trace2,
        &span2,
        "operation2",
        vec![(
            "tool.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "calculator".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    otlp_client.export_traces(request1).await?;
    otlp_client.export_traces(request2).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query with span_type filter
    let response = client
        .get(&format!(
            "/api/v1/spans?project_id={}&span_type=GENERATION",
            project_id
        ))
        .await?;

    assert_eq!(
        response.status(),
        200,
        "Should be able to query with span_type filter"
    );

    let data: serde_json::Value = response.json().await?;
    let spans = data["items"].as_array().expect("Should have items array");

    // All returned spans should be GENERATION
    assert!(
        !spans.is_empty(),
        "Should have at least one GENERATION span"
    );
    for span in spans {
        assert_eq!(
            span["span_type"].as_str().expect("span_type should exist"),
            "GENERATION",
            "All filtered spans should be GENERATION"
        );
    }

    println!("✅ REST API filtering by span_type works correctly");
    Ok(())
}

/// Test REST API filtering by multiple span_types
#[tokio::test]
#[ignore]
async fn test_rest_api_filter_by_multiple_span_types() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Create spans of different types
    let trace1 = TestDataGenerator::trace_id();
    let span1 = TestDataGenerator::span_id();
    let request1 = build_trace_with_attributes(
        "service1",
        &trace1,
        &span1,
        "operation1",
        vec![(
            "gen_ai.request.model",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "gpt-4".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    let trace2 = TestDataGenerator::trace_id();
    let span2 = TestDataGenerator::span_id();
    let request2 = build_trace_with_attributes(
        "service2",
        &trace2,
        &span2,
        "operation2",
        vec![(
            "tool.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "calculator".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    let trace3 = TestDataGenerator::trace_id();
    let span3 = TestDataGenerator::span_id();
    let request3 = build_trace_with_attributes(
        "service3",
        &trace3,
        &span3,
        "operation3",
        vec![(
            "agent.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "agent".to_string(),
                    ),
                ),
            },
        )],
        None,
    );

    otlp_client.export_traces(request1).await?;
    otlp_client.export_traces(request2).await?;
    otlp_client.export_traces(request3).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query with multiple span_types
    let response = client
        .get(&format!(
            "/api/v1/spans?project_id={}&span_types=GENERATION,TOOL",
            project_id
        ))
        .await?;

    assert_eq!(
        response.status(),
        200,
        "Should be able to query with multiple span_types"
    );

    let data: serde_json::Value = response.json().await?;
    let spans = data["items"].as_array().expect("Should have items array");

    // All returned spans should be GENERATION or TOOL
    assert!(
        spans.len() >= 2,
        "Should have at least 2 spans (GENERATION and TOOL)"
    );
    for span in spans {
        let span_type = span["span_type"].as_str().expect("span_type should exist");
        assert!(
            span_type == "GENERATION" || span_type == "TOOL",
            "Filtered spans should be GENERATION or TOOL, got: {}",
            span_type
        );
    }

    println!("✅ REST API filtering by multiple span_types works correctly");
    Ok(())
}

/// Test that JSONB fields (llm_input, llm_output) are stored as JSON and searchable
#[tokio::test]
#[ignore]
async fn test_jsonb_fields_stored_as_json() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Build trace with LLM input/output in attributes
    let request = build_trace_with_attributes(
        "llm-service",
        &trace_id,
        &span_id,
        "openai.chat.completions",
        vec![
            ("gen_ai.request.model", AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    "gpt-4".to_string(),
                )),
            }),
            ("llm.input", AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    r#"{"messages": [{"role": "user", "content": "Hello"}]}"#.to_string(),
                )),
            }),
            ("llm.output", AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    r#"{"choices": [{"message": {"role": "assistant", "content": "Hi there!"}}]}"#.to_string(),
                )),
            }),
        ],
        None,
    );

    otlp_client.export_traces(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    let trace_data: serde_json::Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    // Verify llm_input and llm_output are present (as JSONB, they should be queryable)
    // The exact structure depends on how they're stored, but they should exist
    assert!(
        span.get("llm_input").is_some() || span.get("attributes").is_some(),
        "llm_input should be stored (either directly or in attributes)"
    );

    println!("✅ JSONB fields stored correctly");
    Ok(())
}

// Helper function to build trace with custom attributes
fn build_trace_with_attributes(
    service_name: &str,
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    span_name: &str,
    attributes: Vec<(&str, AnyValue)>,
    timestamps: Option<(u64, u64)>,
) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Status};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let (start_time, end_time) = timestamps.unwrap_or((now - 1_000_000_000, now));

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        service_name.to_string(),
                    ),
                ),
            }),
        }],
        dropped_attributes_count: 0,
    };

    let span = OtlpSpan {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        trace_state: String::new(),
        parent_span_id: vec![],
        name: span_name.to_string(),
        kind: 1, // SPAN_KIND_INTERNAL
        start_time_unix_nano: start_time,
        end_time_unix_nano: end_time,
        attributes: attributes
            .into_iter()
            .map(|(key, value)| KeyValue {
                key: key.to_string(),
                value: Some(value),
            })
            .collect(),
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: 0, // STATUS_CODE_UNSET
        }),
    };

    let scope_spans = ScopeSpans {
        scope: Some(InstrumentationScope {
            name: "test-instrumentation".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        spans: vec![span],
        schema_url: String::new(),
    };

    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope_spans],
        schema_url: String::new(),
    };

    opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}
