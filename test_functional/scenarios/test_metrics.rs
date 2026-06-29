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
async fn test_gauge_metric_ingestion_and_retrieval_body(env: TestEnv) -> Result<()> {
    let request = env
        .otlp
        .build_gauge_metric("test-service", "cpu.usage", 42.5);
    env.otlp.export_metrics(request).await?;
    println!("✅ Gauge metric sent via OTLP");

    let url = "/api/v1/metrics?metric_name=cpu.usage";
    let items = wait_for_items_default(&env.client, url).await?;

    let m = &items[0];
    assert_eq!(m["metric_name"].as_str().unwrap(), "cpu.usage");
    assert_eq!(m["metric_type"].as_str().unwrap(), "GAUGE");
    assert_eq!(m["service_name"].as_str().unwrap(), "test-service");

    let value = m["value"].as_f64().expect("value must be f64");
    assert!(
        (value - 42.5).abs() < 0.001,
        "Expected value ~42.5, got {}",
        value
    );

    println!("✅ Gauge metric retrieved and verified");
    Ok(())
}

dual_transport_test!(
    test_gauge_metric_ingestion_and_retrieval,
    test_gauge_metric_ingestion_and_retrieval_body
);

/// Ingest a counter metric and verify metric_type is COUNTER
async fn test_counter_metric_type_body(env: TestEnv) -> Result<()> {
    let request = env
        .otlp
        .build_counter_metric("test-service", "requests.total", 100.0);
    env.otlp.export_metrics(request).await?;

    let url = "/api/v1/metrics?metric_name=requests.total";
    let items = wait_for_items_default(&env.client, url).await?;

    assert_eq!(items[0]["metric_type"].as_str().unwrap(), "COUNTER");

    println!("✅ Counter metric type verified");
    Ok(())
}

dual_transport_test!(test_counter_metric_type, test_counter_metric_type_body);

/// Ingest multiple different metric names and verify they are all stored
async fn test_multiple_metric_names_body(env: TestEnv) -> Result<()> {
    let metric_names = ["mem.usage", "disk.io", "net.bytes"];
    for name in &metric_names {
        let request = env.otlp.build_gauge_metric("test-service", name, 1.0);
        env.otlp.export_metrics(request).await?;
    }

    // Verify each metric was stored via polling
    for name in &metric_names {
        let url = format!("/api/v1/metrics?metric_name={}", name);
        let items = wait_for_items_default(&env.client, &url).await?;
        assert!(!items.is_empty(), "metric {} should have data points", name);
    }

    println!("✅ Multiple metric names stored and retrieved");
    Ok(())
}

dual_transport_test!(test_multiple_metric_names, test_multiple_metric_names_body);

/// Filter metrics by metric_name returns only matching metrics
async fn test_metric_name_filter_body(env: TestEnv) -> Result<()> {
    env.otlp
        .export_metrics(env.otlp.build_gauge_metric("svc", "metric.alpha", 1.0))
        .await?;
    env.otlp
        .export_metrics(env.otlp.build_gauge_metric("svc", "metric.beta", 2.0))
        .await?;

    let url = "/api/v1/metrics?metric_name=metric.alpha";
    let items = wait_for_items_default(&env.client, url).await?;

    // All returned items must be for metric.alpha only
    for item in &items {
        assert_eq!(
            item["metric_name"].as_str().unwrap(),
            "metric.alpha",
            "metric_name filter must not return other metrics"
        );
    }

    println!("✅ metric_name filter works correctly");
    Ok(())
}

dual_transport_test!(test_metric_name_filter, test_metric_name_filter_body);

/// Filter metrics by service_name returns only matching metrics
async fn test_metric_service_name_filter_body(env: TestEnv) -> Result<()> {
    env.otlp
        .export_metrics(env.otlp.build_gauge_metric("service-a", "cpu.usage", 10.0))
        .await?;
    env.otlp
        .export_metrics(env.otlp.build_gauge_metric("service-b", "cpu.usage", 20.0))
        .await?;

    let url = "/api/v1/metrics?service_name=service-a";
    let items = wait_for_items_default(&env.client, url).await?;

    for item in &items {
        assert_eq!(
            item["service_name"].as_str().unwrap(),
            "service-a",
            "service_name filter must not return other services"
        );
    }

    println!("✅ service_name filter works correctly");
    Ok(())
}

dual_transport_test!(
    test_metric_service_name_filter,
    test_metric_service_name_filter_body
);

/// Metric series endpoint returns time-series points for a named metric
async fn test_metric_series_query_body(env: TestEnv) -> Result<()> {
    // Send a few data points
    for i in 0..3 {
        let request = env
            .otlp
            .build_gauge_metric("test-service", "latency.p99", (i as f64) * 10.0);
        env.otlp.export_metrics(request).await?;
    }

    // Wait for at least one metric point to land before querying series
    let items_url = "/api/v1/metrics?metric_name=latency.p99";
    wait_for_items_default(&env.client, items_url).await?;

    let series_url = "/api/v1/metrics/series?metric_name=latency.p99";
    let response = env.client.get(series_url).await?;
    let status = response.status();
    if status != 200 {
        let body = response.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, body);
    }

    let series: Value = response.json().await?;
    let points = series.as_array().expect("series must be an array");
    assert!(
        !points.is_empty(),
        "Expected at least one time-series point"
    );

    for point in points {
        assert!(
            point["timestamp"].is_string() || point["timestamp"].is_number(),
            "point must have timestamp"
        );
        assert!(point["value"].is_number(), "point must have value");
    }

    println!("✅ Metric series query returned {} points", points.len());
    Ok(())
}

dual_transport_test!(test_metric_series_query, test_metric_series_query_body);

/// Metrics from different workspaces are isolated (workspace isolation)
async fn test_metrics_workspace_isolation_body(_env: TestEnv) -> Result<()> {
    // Provision two independent environments
    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;

    let request = env_a
        .otlp
        .build_gauge_metric("svc", "isolated.metric", 99.0);
    env_a.otlp.export_metrics(request).await?;

    // Wait for workspace A's metric to land
    let url_a = "/api/v1/metrics?metric_name=isolated.metric";
    wait_for_items_default(&env_a.client, url_a).await?;

    // Project B must not see Project A's metrics
    let url_b = "/api/v1/metrics?metric_name=isolated.metric";
    let response = env_b.client.get(url_b).await?;
    assert_eq!(response.status(), 200);
    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(
        items.is_empty(),
        "Project B must not see Project A's metrics"
    );

    println!("✅ Metrics workspace isolation verified");
    Ok(())
}

dual_transport_test!(
    test_metrics_workspace_isolation,
    test_metrics_workspace_isolation_body
);

// ============================================================================
// Negative Tests
// ============================================================================

/// Metrics export without API key is rejected
#[tokio::test]
#[ignore]
async fn test_metrics_export_requires_api_key() -> Result<()> {
    let session = TestSession::setup().await?;
    let no_auth = OtlpClient::new(session.ctx.config.grpc_url.clone());

    let request = no_auth.build_gauge_metric("svc", "cpu.usage", 1.0);
    let result = no_auth.export_metrics(request).await;
    assert!(result.is_err(), "Export without API key must fail");

    println!("✅ Unauthenticated metrics export correctly rejected");
    Ok(())
}

/// Metrics export with invalid API key is rejected
#[tokio::test]
#[ignore]
async fn test_metrics_export_invalid_api_key_rejected() -> Result<()> {
    let session = TestSession::setup().await?;
    let bad_client = OtlpClient::new(session.ctx.config.grpc_url.clone())
        .with_api_key("invalid-key-that-does-not-exist".to_string());

    let request = bad_client.build_gauge_metric("svc", "cpu.usage", 1.0);
    let result = bad_client.export_metrics(request).await;
    assert!(result.is_err(), "Export with invalid API key must fail");

    println!("✅ Invalid API key correctly rejected for metrics");
    Ok(())
}

/// Metrics series without metric_name returns 400/422
async fn test_metric_series_requires_metric_name_body(env: TestEnv) -> Result<()> {
    // Missing metric_name
    let url = "/api/v1/metrics/series".to_string();
    let response = env.client.get(&url).await?;
    let status = response.status().as_u16();
    assert!(
        status == 400 || status == 422,
        "Expected 400 or 422 when metric_name is missing, got {}",
        status
    );

    println!("✅ Metric series without metric_name correctly rejected");
    Ok(())
}

dual_transport_test!(
    test_metric_series_requires_metric_name,
    test_metric_series_requires_metric_name_body
);
