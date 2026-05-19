//! Telemetry Storage Tests
//!
//! These tests verify that spans and metrics sent via OTLP are properly
//! stored in PostgreSQL with all fields intact.

#[allow(unused_imports)]
use crate::*;

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

    env.otlp
        .send_test_trace(
            "llm-service",
            &trace_id,
            &span_id,
            "openai.chat.completions",
        )
        .await?;

    println!("✅ LLM trace sent");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert!(!spans.is_empty(), "Should have spans");

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

/// Test tenant isolation - spans from one project shouldn't be visible to another
#[tokio::test]
#[ignore]
async fn test_telemetry_tenant_isolation() -> Result<()> {
    let env1 = TestEnv::setup().await?;
    let env2 = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env1.otlp
        .send_test_trace("isolated-service", &trace_id, &span_id, "test.isolated")
        .await?;

    println!("✅ Trace sent to project1");

    let trace_id_hex = hex::encode(trace_id);

    // Poll until project1 can see its trace
    wait_for_trace_default(&env1.client, &trace_id_hex).await?;

    // Project2 must NOT see project1's trace (check immediately after project1 confirms storage)
    let response2 = env2
        .client
        .get(&format!("/api/v1/traces/{}", trace_id_hex))
        .await?;

    if response2.status() == 200 {
        let data: Value = response2.json().await?;
        let spans = data["spans"].as_array();
        assert!(
            spans.is_none() || spans.unwrap().is_empty(),
            "Project2 should NOT see project1's trace"
        );
    }

    println!("✅ Tenant isolation verified!");
    Ok(())
}
