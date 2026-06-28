//! Telemetry Storage Tests
//!
//! These tests verify that spans and metrics sent via OTLP are properly
//! stored as Parquet data and can be queried back with all fields intact.

#[allow(unused_imports)]
use crate::*;
use opentelemetry_proto::tonic::common::v1::AnyValue;

fn string_value(value: &str) -> AnyValue {
    AnyValue {
        value: Some(
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                value.to_string(),
            ),
        ),
    }
}

fn int_value(value: i64) -> AnyValue {
    AnyValue {
        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(value)),
    }
}

/// Test that a span sent via OTLP is stored and can be queried back
#[tokio::test]
#[ignore]
async fn test_span_storage_and_retrieval() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = "test-llm-service";
    let span_name = "llm.completion";

    env.otlp
        .send_test_trace(service_name, &trace_id, &span_id, span_name)
        .await?;

    println!("✅ Trace sent via OTLP");

    // Poll until trace appears in storage
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let spans = trace_data["spans"]
        .as_array()
        .expect("Response should have spans array");
    assert!(!spans.is_empty(), "Should have at least 1 span");

    let span = &spans[0];
    assert_eq!(
        span["trace_id"].as_str().unwrap(),
        trace_id_hex,
        "trace_id should match"
    );
    assert_eq!(
        span["service_name"].as_str().unwrap(),
        service_name,
        "service_name should match"
    );
    assert_eq!(
        span["operation_name"].as_str().unwrap(),
        span_name,
        "operation_name should match"
    );

    println!("✅ Span storage verified - all fields match!");
    Ok(())
}

/// Test that LLM-specific span fields are stored correctly
#[tokio::test]
#[ignore]
async fn test_llm_span_fields_storage() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let request = env.otlp.build_multi_span_trace_with_attributes(
        "llm-service",
        &trace_id,
        vec![SpanDefExt {
            name: "openai.chat.completions".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("gen_ai.request.model".to_string(), string_value("gpt-4")),
                ("llm.input".to_string(), string_value("What is zradar?")),
                (
                    "llm.output".to_string(),
                    string_value("zradar is an observability platform."),
                ),
                ("llm.usage.prompt_tokens".to_string(), int_value(11)),
                ("llm.usage.completion_tokens".to_string(), int_value(17)),
                ("llm.usage.total_tokens".to_string(), int_value(28)),
            ],
            status_code: Some(1),
        }],
    );
    env.otlp.export_traces(request).await?;

    println!("✅ LLM trace sent");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert_eq!(spans.len(), 1, "Should have exactly one LLM span");
    let span = &spans[0];
    assert_eq!(
        span["operation_name"].as_str().unwrap(),
        "openai.chat.completions"
    );
    assert_eq!(span["span_type"].as_str().unwrap(), "GENERATION");

    let filtered = wait_for_items_default(&env.client, "/api/v1/spans?llm_model=gpt-4").await?;
    assert!(
        filtered.iter().any(|item| {
            item["span_id"].as_str() == Some(hex::encode(span_id).as_str())
                && item["operation_name"].as_str() == Some("openai.chat.completions")
                && item["span_type"].as_str() == Some("GENERATION")
        }),
        "LLM model should be stored in typed columns and queryable by llm_model"
    );

    println!("✅ LLM span fields verified!");
    Ok(())
}

/// Test querying traces with filters
#[tokio::test]
#[ignore]
async fn test_trace_query_with_filters() -> Result<()> {
    let env = TestEnv::setup().await?;

    let service1 = "api-gateway";
    let service2 = "llm-service";

    for service in [service1, service2] {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        env.otlp
            .send_test_trace(service, &trace_id, &span_id, "test.operation")
            .await?;
    }

    println!("✅ Multiple traces sent");

    // Poll until service1's trace appears in the filtered list
    let filter_url = format!("/api/v1/traces?service_name={}", service1);
    let items = wait_for_items_default(&env.client, &filter_url).await?;

    // All returned traces must be from service1
    for trace in &items {
        assert_eq!(trace["service_name"].as_str().unwrap(), service1);
    }

    println!("✅ Trace filtering verified!");
    Ok(())
}

/// Test that spans with parent-child relationships are stored correctly
#[tokio::test]
#[ignore]
async fn test_span_hierarchy_storage() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let parent_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();

    let spans = vec![
        ("api.request", &parent_span_id, None::<&[u8; 8]>),
        ("db.query", &child_span_id, Some(&parent_span_id)),
    ];

    let request = env
        .otlp
        .build_multi_span_trace("test-service", &trace_id, spans);
    env.otlp.export_traces(request).await?;

    println!("✅ Multi-span trace sent");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans_result = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(spans_result.len(), 2, "Should have 2 spans");

    let child = spans_result
        .iter()
        .find(|s| s["operation_name"].as_str() == Some("db.query"))
        .expect("Should have child span");

    let parent_id_hex = hex::encode(parent_span_id);
    assert_eq!(
        child["parent_span_id"].as_str().unwrap(),
        parent_id_hex,
        "Child should reference parent"
    );

    println!("✅ Span hierarchy verified!");
    Ok(())
}

/// Test workspace isolation - spans from one workspace shouldn't be visible to another
#[tokio::test]
#[ignore]
async fn test_telemetry_workspace_isolation() -> Result<()> {
    let env1 = TestEnv::setup().await?;
    let env2 = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env1.otlp
        .send_test_trace("isolated-service", &trace_id, &span_id, "test.isolated")
        .await?;

    println!("✅ Trace sent to workspace1");

    let trace_id_hex = hex::encode(trace_id);

    // Poll until workspace1 can see its trace
    wait_for_trace_default(&env1.client, &trace_id_hex).await?;

    // Project2 must NOT see workspace1's trace (check immediately after workspace1 confirms storage)
    let response2 = env2
        .client
        .get(&format!("/api/v1/traces/{}", trace_id_hex))
        .await?;

    if response2.status() == 200 {
        let data: Value = response2.json().await?;
        let spans = data["spans"].as_array();
        assert!(
            spans.is_none() || spans.unwrap().is_empty(),
            "Project2 should NOT see workspace1's trace"
        );
    }

    println!("✅ Tenant isolation verified!");
    Ok(())
}
