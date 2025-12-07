//! OTLP/Tracing ingestion tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_send_single_trace() -> Result<()> {
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
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client and send trace
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    println!("✅ Single trace sent successfully via OTLP");

    // VERIFY: Query back the trace to ensure it was stored
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // Wait for async processing

    let trace_id_hex = hex::encode(trace_id);
    let query_url = format!("/api/v1/traces/{}?project_id={}", trace_id_hex, project_id);

    println!("🔍 Querying trace: {}", query_url);
    let query_response = client.get(&query_url).await?;

    assert_eq!(
        query_response.status(),
        200,
        "Should be able to query stored trace"
    );

    let trace_data: serde_json::Value = query_response.json().await?;
    println!(
        "📊 Retrieved trace: {}",
        serde_json::to_string_pretty(&trace_data)?
    );

    // Verify the trace contains our span
    assert!(trace_data.get("spans").is_some(), "Trace should have spans");
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");
    assert!(!spans.is_empty(), "Should have at least 1 span");

    println!("✅ Trace storage verified - data queryable!");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_send_multi_span_trace() -> Result<()> {
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

    // Create test data
    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let spans = vec![
        ("root.operation", &root_span_id, None),
        ("child.operation", &child_span_id, Some(&root_span_id)),
    ];

    let request = otlp_client.build_multi_span_trace(&service_name, &trace_id, spans);
    otlp_client.export_traces(request).await?;

    println!("✅ Multi-span trace sent successfully");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_send_multiple_traces() -> Result<()> {
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

    let service_name = TestDataGenerator::service_name();

    // Create OTLP client
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Send multiple traces
    for i in 0..5 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let span_name = format!("operation.{}", i);

        otlp_client
            .send_test_trace(&service_name, &trace_id, &span_id, &span_name)
            .await?;

        println!("  Trace {}/5 sent", i + 1);
    }

    println!("✅ Multiple traces sent successfully");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_without_api_key_rejected() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client WITHOUT API key
    let otlp_client = OtlpClient::new(ctx.config.grpc_url.clone());

    // Should fail without API key
    let result = otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await;

    assert!(result.is_err(), "Should reject trace without API key");

    println!("✅ Trace without API key rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_with_invalid_api_key_rejected() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client with invalid API key
    let otlp_client = OtlpClient::new(ctx.config.grpc_url.clone())
        .with_api_key("zvr_invalid_key_12345".to_string());

    // Should fail with invalid key
    let result = otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await;

    assert!(result.is_err(), "Should reject trace with invalid API key");

    println!("✅ Trace with invalid API key rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_with_revoked_api_key_rejected() -> Result<()> {
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
    let key_id = parse_uuid_from_json(&api_key, "id")?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let trace_id1 = TestDataGenerator::trace_id();
    let span_id1 = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Send a trace (should work)
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let result1 = otlp_client
        .send_test_trace(&service_name, &trace_id1, &span_id1, "before.revoke")
        .await;
    assert!(result1.is_ok(), "Should work before revocation");

    // Revoke the key
    client.revoke_api_key(&key_id).await?;

    let trace_id2 = TestDataGenerator::trace_id();
    let span_id2 = TestDataGenerator::span_id();

    let result2 = otlp_client
        .send_test_trace(&service_name, &trace_id2, &span_id2, "after.revoke")
        .await;

    assert!(result2.is_err(), "Should reject trace with revoked API key");

    println!("✅ Trace with revoked API key rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_with_different_service_names() -> Result<()> {
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

    // Send traces from different services
    let services = vec!["frontend", "backend", "database", "cache"];

    for service in &services {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        otlp_client
            .send_test_trace(service, &trace_id, &span_id, "test.operation")
            .await?;

        println!("  Trace from service '{}' sent", service);
    }

    println!("✅ Traces from multiple services accepted");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_with_complex_attributes() -> Result<()> {
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

    // The test_trace includes HTTP attributes
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "GET /api/users")
        .await?;

    println!("✅ Trace with attributes accepted");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_high_volume_trace_ingestion() -> Result<()> {
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

    let service_name = TestDataGenerator::service_name();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());
    let count = 50; // Send 50 traces

    let start = std::time::Instant::now();

    for i in 0..count {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let span_name = format!("operation.{}", i);

        otlp_client
            .send_test_trace(&service_name, &trace_id, &span_id, &span_name)
            .await?;

        if (i + 1) % 10 == 0 {
            println!("  Sent {}/{} traces", i + 1, count);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "✅ {} traces ingested in {:?} ({:.2} traces/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concurrent_trace_ingestion() -> Result<()> {
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
    let key_value = get_string_from_json(&api_key, "key")?.to_string();

    let grpc_url = ctx.config.grpc_url.clone();
    let service_name = TestDataGenerator::service_name();

    // Send traces concurrently
    let mut handles = vec![];

    for i in 0..10 {
        let key = key_value.clone();
        let url = grpc_url.clone();
        let svc = service_name.clone();

        let handle = tokio::spawn(async move {
            let otlp_client = OtlpClient::new(url).with_api_key(key);
            let trace_id = TestDataGenerator::trace_id();
            let span_id = TestDataGenerator::span_id();
            let span_name = format!("concurrent.{}", i);

            otlp_client
                .send_test_trace(&svc, &trace_id, &span_id, &span_name)
                .await
        });

        handles.push(handle);
    }

    // Wait for all to complete
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent trace {} failed", i);
    }

    println!("✅ Concurrent trace ingestion successful");
    Ok(())
}
