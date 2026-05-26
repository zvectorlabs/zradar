//! Query API E2E tests

#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_query_spans_with_filters() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let service_name = TestDataGenerator::service_name();

    let span1_id = TestDataGenerator::span_id();
    let span2_id = TestDataGenerator::span_id();
    let span3_id = TestDataGenerator::span_id();

    let span_defs = vec![
        ("root.span", &span1_id, None),
        ("db.query", &span2_id, Some(&span1_id)),
        ("cache.get", &span3_id, Some(&span1_id)),
    ];

    let request = env
        .otlp
        .build_multi_span_trace(&service_name, &trace_id, span_defs);
    env.otlp.export_traces(request).await?;

    // Poll until all 3 spans appear
    let trace_id_hex = hex::encode(trace_id);
    let query_path = format!("/api/v1/spans?trace_id={}", trace_id_hex);
    let data = wait_for_items_default(&env.client, &query_path).await?;

    assert!(
        data.len() >= 3,
        "Expected at least 3 spans, got {}",
        data.len()
    );

    for span in &data {
        assert!(span.get("span_id").is_some(), "Span should have span_id");
        assert!(span.get("trace_id").is_some(), "Span should have trace_id");
        assert!(
            span.get("operation_name").is_some(),
            "Span should have operation_name"
        );
        assert!(
            span.get("duration_ms").is_some(),
            "Span should have duration_ms"
        );
    }

    // Test filtering by operation name (data is already stored, no extra wait needed)
    let filtered_path = format!(
        "/api/v1/spans?trace_id={}&operation_name=db.query",
        trace_id_hex
    );
    let filtered_data = wait_for_items_default(&env.client, &filtered_path).await?;

    assert!(
        !filtered_data.is_empty(),
        "Expected at least 1 filtered span"
    );
    for span in &filtered_data {
        let name = span
            .get("operation_name")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        assert!(
            name.contains("db.query"),
            "Filtered span name should contain 'db.query', got: {}",
            name
        );
    }

    println!("✅ Span query test passed");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_query_traces_with_attribute_filters() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    env.otlp
        .send_test_trace("test-service", &trace_id, &span_id, "test.operation")
        .await?;

    // Poll until the trace appears in the time-ranged query
    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let now = chrono::Utc::now();
    let start_rfc = (now - chrono::Duration::hours(1)).to_rfc3339();
    let end_rfc = now.to_rfc3339();
    let start_time = urlencoding::encode(&start_rfc);
    let end_time = urlencoding::encode(&end_rfc);

    let basic_path = format!(
        "/api/v1/traces?start_time={}&end_time={}",
        start_time, end_time
    );

    let response = env.client.get(&basic_path).await?;
    assert_eq!(response.status(), 200, "Basic query should succeed");

    let body: Value = response.json().await?;
    let items = body
        .get("items")
        .and_then(|d| d.as_array())
        .expect("Expected 'items' array in response");
    assert!(!items.is_empty(), "Expected at least 1 trace");

    println!("✅ Attribute filter query structure test passed");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_error_analytics() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    // Ensure the trace is ingested before querying analytics
    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let now = chrono::Utc::now();
    let start_rfc = (now - chrono::Duration::hours(1)).to_rfc3339();
    let end_rfc = now.to_rfc3339();
    let start_time = urlencoding::encode(&start_rfc);
    let end_time = urlencoding::encode(&end_rfc);

    let error_path = format!(
        "/api/v1/analytics/errors?start_time={}&end_time={}",
        start_time, end_time
    );

    let response = env.client.get(&error_path).await?;
    let status = response.status();
    if status != 200 {
        let error_text = response.text().await.unwrap_or_default();
        panic!(
            "Expected 200 OK for error analytics, got {}: {}",
            status, error_text
        );
    }

    let body: Value = response.json().await?;
    let breakdowns = body.as_array().expect("Expected array response");
    for breakdown in breakdowns {
        assert!(
            breakdown
                .get("error_type")
                .and_then(|value| value.as_str())
                .is_some(),
            "Each error breakdown should include error_type"
        );
        assert!(
            breakdown
                .get("count")
                .and_then(|value| value.as_i64())
                .is_some(),
            "Each error breakdown should include numeric count"
        );
        assert!(
            breakdown
                .get("percentage")
                .and_then(|value| value.as_f64())
                .is_some(),
            "Each error breakdown should include numeric percentage"
        );
    }
    println!("Found {} error types", breakdowns.len());

    println!("✅ Error analytics test passed");
    Ok(())
}
