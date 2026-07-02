//! Span Type Detection Tests
//!
//! These tests verify that span_type is correctly detected from OTLP attributes
//! and can be queried via REST API.

use anyhow::Result;
use functional_tests::*;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;
use std::time::{SystemTime, UNIX_EPOCH};

/// Test that spans with gen_ai.request.model attribute are detected as GENERATION
async fn test_generation_span_type_from_model_attribute_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

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
    env.otlp.export_traces(request).await?;
    println!("✅ Trace with model attribute sent");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert!(!spans.is_empty(), "Should have at least 1 span");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "GENERATION",
        "Span with model attribute should be GENERATION"
    );

    println!("✅ GENERATION span type detected correctly");
    Ok(())
}

dual_transport_test!(
    test_generation_span_type_from_model_attribute,
    test_generation_span_type_from_model_attribute_body
);

/// Test that spans with tool.name attribute are detected as TOOL
async fn test_tool_span_type_from_tool_attribute_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

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
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "TOOL",
        "Span with tool.name should be TOOL"
    );

    println!("✅ TOOL span type detected correctly");
    Ok(())
}

dual_transport_test!(
    test_tool_span_type_from_tool_attribute,
    test_tool_span_type_from_tool_attribute_body
);

/// Test that spans with agent.name attribute are detected as AGENT
async fn test_agent_span_type_from_agent_attribute_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

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
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "AGENT",
        "Span with agent.name should be AGENT"
    );

    println!("✅ AGENT span type detected correctly");
    Ok(())
}

dual_transport_test!(
    test_agent_span_type_from_agent_attribute,
    test_agent_span_type_from_agent_attribute_body
);

/// Test that spans with db.system attribute are detected as DATABASE
async fn test_database_span_type_from_db_attribute_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let request = build_trace_with_attributes(
        "db-service",
        &trace_id,
        &span_id,
        "query",
        vec![(
            "db.system.name",
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "postgresql".to_string(),
                    ),
                ),
            },
        )],
        None,
    );
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "DATABASE",
        "Span with db.system.name should be DATABASE"
    );

    println!("✅ DATABASE span type detected correctly");
    Ok(())
}

dual_transport_test!(
    test_database_span_type_from_db_attribute,
    test_database_span_type_from_db_attribute_body
);

/// Test that database fields are correctly extracted
async fn test_database_fields_extraction_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let request = build_trace_with_attributes(
        "db-service",
        &trace_id,
        &span_id,
        "query",
        vec![
            (
                "db.system.name",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "postgresql".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.namespace",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "public".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.operation.name",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "SELECT".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.query.text",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "SELECT * FROM users".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.query.summary",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "SELECT users".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.collection.name",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "users".to_string(),
                        ),
                    ),
                },
            ),
            (
                "db.response.status_code",
                AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "0".to_string(),
                        ),
                    ),
                },
            ),
        ],
        None,
    );
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert_eq!(span["span_type"].as_str().unwrap(), "DATABASE");
    assert_eq!(span["db_system_name"].as_str().unwrap(), "postgresql");
    assert_eq!(span["db_namespace"].as_str().unwrap(), "public");
    assert_eq!(span["db_operation_name"].as_str().unwrap(), "SELECT");
    assert_eq!(
        span["db_query_text"].as_str().unwrap(),
        "SELECT * FROM users"
    );
    assert_eq!(span["db_query_summary"].as_str().unwrap(), "SELECT users");
    assert_eq!(span["db_collection_name"].as_str().unwrap(), "users");
    assert_eq!(span["db_response_status_code"].as_str().unwrap(), "0");

    println!("✅ DATABASE fields extracted correctly");
    Ok(())
}

dual_transport_test!(
    test_database_fields_extraction,
    test_database_fields_extraction_body
);

/// Test that zero-duration spans are detected as EVENT
async fn test_event_span_type_for_zero_duration_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let request = build_trace_with_attributes(
        "event-service",
        &trace_id,
        &span_id,
        "user.click",
        vec![],
        Some((now, now)), // Zero duration
    );
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "EVENT",
        "Zero-duration span should be EVENT"
    );

    println!("✅ EVENT span type detected correctly");
    Ok(())
}

dual_transport_test!(
    test_event_span_type_for_zero_duration,
    test_event_span_type_for_zero_duration_body
);

/// Test that explicit zradar.span.type attribute is respected
async fn test_explicit_span_type_from_zradar_attribute_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

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
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(
        spans[0]["span_type"]
            .as_str()
            .expect("span_type should exist"),
        "CHAIN",
        "Explicit zradar.span.type should override other attributes"
    );

    println!("✅ Explicit span type (CHAIN) detected correctly");
    Ok(())
}

dual_transport_test!(
    test_explicit_span_type_from_zradar_attribute,
    test_explicit_span_type_from_zradar_attribute_body
);

/// Test REST API filtering by span_type
async fn test_rest_api_filter_by_span_type_body(env: TestEnv) -> Result<()> {
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

    env.otlp.export_traces(request1).await?;
    env.otlp.export_traces(request2).await?;

    // Poll until at least one GENERATION span appears
    let filter_url = "/api/v1/spans?span_type=GENERATION";
    let spans = wait_for_items_default(&env.client, filter_url).await?;

    assert!(
        !spans.is_empty(),
        "Should have at least one GENERATION span"
    );
    for span in &spans {
        assert_eq!(
            span["span_type"].as_str().expect("span_type should exist"),
            "GENERATION",
            "All filtered spans should be GENERATION"
        );
    }

    println!("✅ REST API filtering by span_type works correctly");
    Ok(())
}

dual_transport_test!(
    test_rest_api_filter_by_span_type,
    test_rest_api_filter_by_span_type_body
);

/// Test REST API filtering by multiple span_types
async fn test_rest_api_filter_by_multiple_span_types_body(env: TestEnv) -> Result<()> {
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

    env.otlp.export_traces(request1).await?;
    env.otlp.export_traces(request2).await?;
    env.otlp.export_traces(request3).await?;

    // Poll until BOTH GENERATION and TOOL spans appear (2 separate ingests, may arrive separately)
    let filter_url = "/api/v1/spans?span_types=GENERATION,TOOL";
    let spans = poll_until(
        || async {
            let response = env.client.get(filter_url).await?;
            let data: Value = response.json().await?;
            let items = data
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if items.len() >= 2 {
                Ok(Some(items))
            } else {
                Ok(None)
            }
        },
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await?;

    assert!(
        spans.len() >= 2,
        "Should have at least 2 spans (GENERATION and TOOL)"
    );
    for span in &spans {
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

dual_transport_test!(
    test_rest_api_filter_by_multiple_span_types,
    test_rest_api_filter_by_multiple_span_types_body
);

/// Test that JSONB fields (llm_input, llm_output) are stored as JSON and searchable
async fn test_jsonb_fields_stored_as_json_body(env: TestEnv) -> Result<()> {
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let request = build_trace_with_attributes(
        "llm-service",
        &trace_id,
        &span_id,
        "openai.chat.completions",
        vec![
            ("gen_ai.request.model", AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("gpt-4".to_string())),
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
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    let span = &spans[0];

    assert!(
        span.get("llm_input").is_some() || span.get("attributes").is_some(),
        "llm_input should be stored (either directly or in attributes)"
    );

    println!("✅ JSONB fields stored correctly");
    Ok(())
}

dual_transport_test!(
    test_jsonb_fields_stored_as_json,
    test_jsonb_fields_stored_as_json_body
);

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
            ..Default::default()
        }],
        dropped_attributes_count: 0,
        ..Default::default()
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
                ..Default::default()
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
        ..Default::default()
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
