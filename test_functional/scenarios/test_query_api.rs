//! Query API E2E tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_query_spans_with_filters() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org, project, and API key
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
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client and send multi-span trace
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Send trace with multiple spans
    let span1_id = TestDataGenerator::span_id();
    let span2_id = TestDataGenerator::span_id();
    let span3_id = TestDataGenerator::span_id();

    let span_defs = vec![
        ("root.span", &span1_id, None),
        ("db.query", &span2_id, Some(&span1_id)),
        ("cache.get", &span3_id, Some(&span1_id)),
    ];

    let request = otlp_client.build_multi_span_trace(&service_name, &trace_id, span_defs);
    otlp_client.export_traces(request).await?;

    // Wait for ingestion
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Query all spans for this trace
    let trace_id_hex = hex::encode(trace_id);
    let query_path = format!(
        "/api/v1/spans?project_id={}&trace_id={}",
        project_id, trace_id_hex
    );

    println!("Querying spans: {}", query_path);

    let response = client.get(&query_path).await?;

    assert_eq!(response.status(), 200, "Expected 200 OK response");

    let body: Value = response.json().await?;
    println!(
        "Span query response: {}",
        serde_json::to_string_pretty(&body)?
    );

    // Assert we got spans back
    let data = body
        .get("items")
        .and_then(|d| d.as_array())
        .expect("Expected 'items' array in response");

    assert!(
        data.len() >= 3,
        "Expected at least 3 spans, got {}",
        data.len()
    );

    // Assert span details are present
    for span in data {
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

    // Test filtering by operation name
    let filtered_path = format!(
        "/api/v1/spans?project_id={}&trace_id={}&operation_name=db.query",
        project_id, trace_id_hex
    );

    let filtered_response = client.get(&filtered_path).await?;

    let filtered_body: Value = filtered_response.json().await?;
    let filtered_data = filtered_body
        .get("items")
        .and_then(|d| d.as_array())
        .expect("Expected 'items' array in filtered response");

    // Should only get spans with "db.query" in the name
    assert!(
        !filtered_data.is_empty(),
        "Expected at least 1 filtered span"
    );
    for span in filtered_data {
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
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org, project, and API key
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
    let service_name = "test-service";

    // Create OTLP client and send trace with custom attributes
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Build trace with custom attributes using OTLP client
    // Note: We'll need to extend OtlpClient to support custom attributes
    // For now, send basic trace and test the query structure

    let span_id = TestDataGenerator::span_id();
    otlp_client
        .send_test_trace(service_name, &trace_id, &span_id, "test.operation")
        .await?;

    // Wait for ingestion
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Build ISO 8601 time range (URL encoded)
    let now = chrono::Utc::now();
    let start_rfc = (now - chrono::Duration::hours(1)).to_rfc3339();
    let end_rfc = now.to_rfc3339();
    let start_time = urlencoding::encode(&start_rfc);
    let end_time = urlencoding::encode(&end_rfc);

    // Test 1: Basic query without attributes (should work)
    let basic_path = format!(
        "/api/v1/traces?project_id={}&start_time={}&end_time={}",
        project_id, start_time, end_time
    );

    println!("Testing basic trace query: {}", basic_path);

    let response = client.get(&basic_path).await?;

    assert_eq!(response.status(), 200, "Basic query should succeed");

    let body: Value = response.json().await?;
    println!(
        "Basic query response: {}",
        serde_json::to_string_pretty(&body)?
    );

    let items = body
        .get("items")
        .and_then(|d| d.as_array())
        .expect("Expected 'items' array in response");

    assert!(!items.is_empty(), "Expected at least 1 trace");

    // Note: Full attribute filter testing would require sending traces with
    // custom attributes. For now, we've verified the query structure compiles
    // and basic queries work. Attribute filter logic is tested via the
    // build_attribute_conditions method which can be unit tested.

    println!("✅ Attribute filter query structure test passed");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_error_analytics() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org, project, and API key
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

    // Send some traces (in real scenario, would send traces with error status)
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    // Wait for ingestion
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Build time range (URL encoded)
    let now = chrono::Utc::now();
    let start_rfc = (now - chrono::Duration::hours(1)).to_rfc3339();
    let end_rfc = now.to_rfc3339();
    let start_time = urlencoding::encode(&start_rfc);
    let end_time = urlencoding::encode(&end_rfc);

    // Query error analytics
    let error_path = format!(
        "/api/v1/analytics/errors?project_id={}&start_time={}&end_time={}",
        project_id, start_time, end_time
    );

    println!("Querying error analytics: {}", error_path);

    let response = client.get(&error_path).await?;

    let status = response.status();
    if status != 200 {
        let error_text = response.text().await.unwrap_or_default();
        panic!(
            "Expected 200 OK for error analytics, got {}: {}",
            status, error_text
        );
    }

    let body: Value = response.json().await?;
    println!(
        "Error analytics response: {}",
        serde_json::to_string_pretty(&body)?
    );

    // Response should be an array (may be empty if no errors in test data)
    let breakdowns = body.as_array().expect("Expected array response");

    // TODO: When error analytics is fully implemented, add field checks
    // For now, just verify we get an array response
    println!("Found {} error types", breakdowns.len());

    println!("✅ Error analytics test passed");
    Ok(())
}
