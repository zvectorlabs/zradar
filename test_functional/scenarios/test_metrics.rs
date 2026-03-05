//! Metrics Functional Tests
//!
//! Tests for OTLP metrics ingestion via gRPC and querying via REST API.
//! Covers gauge and counter metrics, filtering, time-series, and auth.

#[allow(unused_imports)]
use crate::*;

// ============================================================================
// Positive Tests
// ============================================================================

/// Ingest a gauge metric and verify it can be retrieved via REST
#[tokio::test]
#[ignore]
async fn test_gauge_metric_ingestion_and_retrieval() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = otlp.build_gauge_metric("test-service", "cpu.usage", 42.5);
    otlp.export_metrics(request).await?;
    println!("✅ Gauge metric sent via OTLP");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/metrics?project_id={}&metric_name=cpu.usage", project_id);
    let response = client.get(&url).await?;
    let status = response.status();
    if status != 200 {
        let body = response.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, body);
    }

    let data: Value = response.json().await?;
    let items = data["items"].as_array().expect("items must be an array");
    assert!(!items.is_empty(), "Expected at least one metric");

    let m = &items[0];
    assert_eq!(m["metric_name"].as_str().unwrap(), "cpu.usage");
    assert_eq!(m["metric_type"].as_str().unwrap(), "GAUGE");
    assert_eq!(m["service_name"].as_str().unwrap(), "test-service");

    let value = m["value"].as_f64().expect("value must be f64");
    assert!((value - 42.5).abs() < 0.001, "Expected value ~42.5, got {}", value);

    println!("✅ Gauge metric retrieved and verified");
    Ok(())
}

/// Ingest a counter metric and verify metric_type is COUNTER
#[tokio::test]
#[ignore]
async fn test_counter_metric_type() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let request = otlp.build_counter_metric("test-service", "requests.total", 100.0);
    otlp.export_metrics(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/metrics?project_id={}&metric_name=requests.total", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().expect("items must be an array");
    assert!(!items.is_empty(), "Expected at least one metric");
    assert_eq!(items[0]["metric_type"].as_str().unwrap(), "COUNTER");

    println!("✅ Counter metric type verified");
    Ok(())
}

/// Ingest multiple different metric names and verify they are all stored
#[tokio::test]
#[ignore]
async fn test_multiple_metric_names() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let metric_names = ["mem.usage", "disk.io", "net.bytes"];
    for name in &metric_names {
        let request = otlp.build_gauge_metric("test-service", name, 1.0);
        otlp.export_metrics(request).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify each metric exists
    for name in &metric_names {
        let url = format!("/api/v1/metrics?project_id={}&metric_name={}", project_id, name);
        let response = client.get(&url).await?;
        assert_eq!(response.status(), 200, "metric {} should be retrievable", name);
        let data: Value = response.json().await?;
        let items = data["items"].as_array().unwrap();
        assert!(!items.is_empty(), "metric {} should have data points", name);
    }

    println!("✅ Multiple metric names stored and retrieved");
    Ok(())
}

/// Filter metrics by metric_name returns only matching metrics
#[tokio::test]
#[ignore]
async fn test_metric_name_filter() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp.export_metrics(otlp.build_gauge_metric("svc", "metric.alpha", 1.0)).await?;
    otlp.export_metrics(otlp.build_gauge_metric("svc", "metric.beta", 2.0)).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/metrics?project_id={}&metric_name=metric.alpha", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    // All returned items must be for metric.alpha only
    for item in items {
        assert_eq!(
            item["metric_name"].as_str().unwrap(),
            "metric.alpha",
            "metric_name filter must not return other metrics"
        );
    }

    println!("✅ metric_name filter works correctly");
    Ok(())
}

/// Filter metrics by service_name returns only matching metrics
#[tokio::test]
#[ignore]
async fn test_metric_service_name_filter() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp.export_metrics(otlp.build_gauge_metric("service-a", "cpu.usage", 10.0)).await?;
    otlp.export_metrics(otlp.build_gauge_metric("service-b", "cpu.usage", 20.0)).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/metrics?project_id={}&service_name=service-a", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    for item in items {
        assert_eq!(
            item["service_name"].as_str().unwrap(),
            "service-a",
            "service_name filter must not return other services"
        );
    }

    println!("✅ service_name filter works correctly");
    Ok(())
}

/// Metric series endpoint returns time-series points for a named metric
#[tokio::test]
#[ignore]
async fn test_metric_series_query() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(&project_id, &fixture.api_key_name, &fixture.api_key_description)
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Send a few data points
    for i in 0..3 {
        let request = otlp.build_gauge_metric("test-service", "latency.p99", (i as f64) * 10.0);
        otlp.export_metrics(request).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/metrics/series?project_id={}&metric_name=latency.p99", project_id);
    let response = client.get(&url).await?;
    let status = response.status();
    if status != 200 {
        let body = response.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, body);
    }

    let series: Value = response.json().await?;
    let points = series.as_array().expect("series must be an array");
    assert!(!points.is_empty(), "Expected at least one time-series point");

    // Each point must have timestamp and value
    for point in points {
        assert!(point["timestamp"].is_string() || point["timestamp"].is_number(),
            "point must have timestamp");
        assert!(point["value"].is_number(), "point must have value");
    }

    println!("✅ Metric series query returned {} points", points.len());
    Ok(())
}

/// Metrics from different projects are isolated (tenant isolation)
#[tokio::test]
#[ignore]
async fn test_metrics_tenant_isolation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture_a = TestFixture::new();
    let fixture_b = TestFixture::new();

    // Project A
    let org_a = client
        .create_organization(&fixture_a.org_name, &fixture_a.org_display_name)
        .await?;
    let org_a_id = parse_uuid_from_json(&org_a, "id")?;
    let proj_a = client
        .create_project(&org_a_id, &fixture_a.project_name, &fixture_a.project_display_name)
        .await?;
    let proj_a_id = parse_uuid_from_json(&proj_a, "id")?;
    let key_a = client
        .create_api_key(&proj_a_id, &fixture_a.api_key_name, &fixture_a.api_key_description)
        .await?;
    let key_a_value = get_string_from_json(&key_a, "key")?;

    // Project B
    let org_b = client
        .create_organization(&fixture_b.org_name, &fixture_b.org_display_name)
        .await?;
    let org_b_id = parse_uuid_from_json(&org_b, "id")?;
    let proj_b = client
        .create_project(&org_b_id, &fixture_b.project_name, &fixture_b.project_display_name)
        .await?;
    let proj_b_id = parse_uuid_from_json(&proj_b, "id")?;

    let otlp_a = OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_a_value.to_string());
    let request = otlp_a.build_gauge_metric("svc", "isolated.metric", 99.0);
    otlp_a.export_metrics(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Project B should not see Project A's metrics
    let url = format!("/api/v1/metrics?project_id={}&metric_name=isolated.metric", proj_b_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);
    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(items.is_empty(), "Project B must not see Project A's metrics");

    println!("✅ Metrics tenant isolation verified");
    Ok(())
}

// ============================================================================
// Negative Tests
// ============================================================================

/// Metrics export without API key is rejected
#[tokio::test]
#[ignore]
async fn test_metrics_export_requires_api_key() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client_no_auth = OtlpClient::new(ctx.config.grpc_url.clone());

    let request = client_no_auth.build_gauge_metric("svc", "cpu.usage", 1.0);
    let result = client_no_auth.export_metrics(request).await;
    assert!(result.is_err(), "Export without API key must fail");

    println!("✅ Unauthenticated metrics export correctly rejected");
    Ok(())
}

/// Metrics export with invalid API key is rejected
#[tokio::test]
#[ignore]
async fn test_metrics_export_invalid_api_key_rejected() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let bad_client = OtlpClient::new(ctx.config.grpc_url.clone())
        .with_api_key("invalid-key-that-does-not-exist".to_string());

    let request = bad_client.build_gauge_metric("svc", "cpu.usage", 1.0);
    let result = bad_client.export_metrics(request).await;
    assert!(result.is_err(), "Export with invalid API key must fail");

    println!("✅ Invalid API key correctly rejected for metrics");
    Ok(())
}

/// Querying metrics without project_id returns 400/422
#[tokio::test]
#[ignore]
async fn test_metrics_query_requires_project_id() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let response = client.get("/api/v1/metrics").await?;
    let status = response.status().as_u16();
    assert!(
        status == 400 || status == 422,
        "Expected 400 or 422 when project_id is missing, got {}",
        status
    );

    println!("✅ Metrics query without project_id correctly rejected");
    Ok(())
}

/// Metrics series without metric_name returns 400/422
#[tokio::test]
#[ignore]
async fn test_metric_series_requires_metric_name() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;
    let fixture = TestFixture::new();

    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(&org_id, &fixture.project_name, &fixture.project_display_name)
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    // Missing metric_name
    let url = format!("/api/v1/metrics/series?project_id={}", project_id);
    let response = client.get(&url).await?;
    let status = response.status().as_u16();
    assert!(
        status == 400 || status == 422,
        "Expected 400 or 422 when metric_name is missing, got {}",
        status
    );

    println!("✅ Metric series without metric_name correctly rejected");
    Ok(())
}
