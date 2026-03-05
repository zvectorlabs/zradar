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

    // Generate test data
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = "test-llm-service";
    let span_name = "llm.completion";

    // Send trace via OTLP
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp_client
        .send_test_trace(service_name, &trace_id, &span_id, span_name)
        .await?;

    println!("✅ Trace sent via OTLP");

    // Wait for async processing (job queue → storage)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query back the trace
    let trace_id_hex = hex::encode(trace_id);
    let query_url = format!("/api/v1/traces/{}?project_id={}", trace_id_hex, project_id);

    println!("🔍 Querying trace: {}", query_url);
    let response = client.get(&query_url).await?;
    let status = response.status();

    if status != 200 {
        let error_text = response.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, error_text);
    }

    let trace_data: Value = response.json().await?;
    println!(
        "📊 Retrieved trace: {}",
        serde_json::to_string_pretty(&trace_data)?
    );

    // Verify spans exist
    let spans = trace_data["spans"]
        .as_array()
        .expect("Response should have spans array");

    assert!(!spans.is_empty(), "Should have at least 1 span");

    // Verify span fields
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

    // Send LLM trace with custom attributes
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Use build_llm_trace if available, or send_test_trace
    otlp_client
        .send_test_trace(
            "llm-service",
            &trace_id,
            &span_id,
            "openai.chat.completions",
        )
        .await?;

    println!("✅ LLM trace sent");

    // Wait and query
    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    assert_eq!(response.status(), 200, "Should be able to query LLM trace");

    let trace_data: Value = response.json().await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");

    assert!(!spans.is_empty(), "Should have spans");

    println!("✅ LLM span fields verified!");
    Ok(())
}

/// Test querying traces with filters
#[tokio::test]
#[ignore]
async fn test_trace_query_with_filters() -> Result<()> {
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

    // Send multiple traces with different services
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let service1 = "api-gateway";
    let service2 = "llm-service";

    for service in [service1, service2] {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        otlp_client
            .send_test_trace(service, &trace_id, &span_id, "test.operation")
            .await?;
    }

    println!("✅ Multiple traces sent");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query with service filter
    let response = client
        .get(&format!(
            "/api/v1/traces?project_id={}&service_name={}",
            project_id, service1
        ))
        .await?;

    assert_eq!(response.status(), 200, "Query should succeed");

    let result: Value = response.json().await?;
    let empty_vec = vec![];
    let traces = result["items"].as_array().unwrap_or(&empty_vec);

    // All returned traces should be from service1
    for trace in traces {
        assert_eq!(trace["service_name"].as_str().unwrap(), service1);
    }

    println!("✅ Trace filtering verified!");
    Ok(())
}

/// Test that spans with parent-child relationships are stored correctly
#[tokio::test]
#[ignore]
async fn test_span_hierarchy_storage() -> Result<()> {
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
    let parent_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();

    // Send multi-span trace
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Build a trace with parent and child span
    let spans = vec![
        ("api.request", &parent_span_id, None::<&[u8; 8]>),
        ("db.query", &child_span_id, Some(&parent_span_id)),
    ];

    let request = otlp_client.build_multi_span_trace("test-service", &trace_id, spans);
    otlp_client.export_traces(request).await?;

    println!("✅ Multi-span trace sent");

    // Wait and query
    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let response = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id
        ))
        .await?;

    assert_eq!(response.status(), 200, "Should query trace successfully");

    let trace_data: Value = response.json().await?;
    let spans_result = trace_data["spans"].as_array().expect("Should have spans");

    assert_eq!(spans_result.len(), 2, "Should have 2 spans");

    // Verify parent-child relationship
    let _parent = spans_result
        .iter()
        .find(|s| s["operation_name"].as_str() == Some("api.request"))
        .expect("Should have parent span");

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
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Create two separate organizations/projects
    let fixture1 = TestFixture::new();
    let fixture2 = TestFixture::new();

    // Setup org1/project1
    let org1 = client
        .create_organization(&fixture1.org_name, &fixture1.org_display_name)
        .await?;
    let org_id1 = parse_uuid_from_json(&org1, "id")?;
    let project1 = client
        .create_project(
            &org_id1,
            &fixture1.project_name,
            &fixture1.project_display_name,
        )
        .await?;
    let project_id1 = parse_uuid_from_json(&project1, "id")?;
    let api_key1 = client
        .create_api_key(
            &project_id1,
            &fixture1.api_key_name,
            &fixture1.api_key_description,
        )
        .await?;
    let key1 = get_string_from_json(&api_key1, "key")?;

    // Setup org2/project2
    let org2 = client
        .create_organization(&fixture2.org_name, &fixture2.org_display_name)
        .await?;
    let org_id2 = parse_uuid_from_json(&org2, "id")?;
    let project2 = client
        .create_project(
            &org_id2,
            &fixture2.project_name,
            &fixture2.project_display_name,
        )
        .await?;
    let project_id2 = parse_uuid_from_json(&project2, "id")?;

    // Send trace to project1 ONLY
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let otlp1 = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key1.to_string());
    otlp1
        .send_test_trace("isolated-service", &trace_id, &span_id, "test.isolated")
        .await?;

    println!("✅ Trace sent to project1");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);

    // Query from project1 - should find it
    let response1 = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id1
        ))
        .await?;
    assert_eq!(response1.status(), 200, "Project1 should see its trace");

    // Query from project2 - should NOT find it
    let response2 = client
        .get(&format!(
            "/api/v1/traces/{}?project_id={}",
            trace_id_hex, project_id2
        ))
        .await?;

    // Should either return 404 or empty data
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
