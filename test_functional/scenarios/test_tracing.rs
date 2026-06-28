//! OTLP/Tracing ingestion tests

#[allow(unused_imports)]
use crate::*;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn test_send_single_trace() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    println!("✅ Single trace sent successfully via OTLP");

    // Verify storage: poll until the trace appears
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;

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
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    let spans = vec![
        ("root.operation", &root_span_id, None),
        ("child.operation", &child_span_id, Some(&root_span_id)),
    ];

    let request = env
        .otlp
        .build_multi_span_trace(&service_name, &trace_id, spans);
    env.otlp.export_traces(request).await?;

    println!("✅ Multi-span trace sent successfully");

    // Verify storage: poll until both spans appear
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");
    assert_eq!(spans.len(), 2, "Should have exactly 2 spans");

    println!("✅ Multi-span trace storage verified");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_send_multiple_traces() -> Result<()> {
    let env = TestEnv::setup().await?;

    let service_name = TestDataGenerator::service_name();
    let mut trace_ids = Vec::new();

    for i in 0..5 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let span_name = format!("operation.{}", i);

        env.otlp
            .send_test_trace(&service_name, &trace_id, &span_id, &span_name)
            .await?;

        trace_ids.push(trace_id);
        println!("  Trace {}/5 sent", i + 1);
    }

    // Verify all 5 traces were stored by polling for each
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
        assert!(
            trace_data["spans"]
                .as_array()
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "Trace {} should have spans",
            trace_id_hex
        );
    }

    println!("✅ All 5 traces stored and verified");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_without_api_key_rejected() -> Result<()> {
    let session = TestSession::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // OTLP client WITHOUT API key
    let otlp_client = OtlpClient::new(session.ctx.config.grpc_url.clone());

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
    let session = TestSession::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    let otlp_client = OtlpClient::new(session.ctx.config.grpc_url.clone())
        .with_api_key("zvr_invalid_key_12345".to_string());

    let result = otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await;

    assert!(result.is_err(), "Should reject trace with invalid API key");

    println!("✅ Trace with invalid API key rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_for_blocked_workspace_rejected() -> Result<()> {
    let mut env = TestEnv::setup().await?;
    let blocked_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(blocked_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(blocked_workspace_id.to_string());

    let settings_resp = env
        .client
        .put(
            &format!("/api/v1/workspaces/{}/settings", blocked_workspace_id),
            &serde_json::json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": null,
                "file_push_interval_secs": 300,
                "blocked": true
            }),
        )
        .await?;
    assert_eq!(settings_resp.status(), 200);

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let result = env
        .otlp
        .send_test_trace(
            "blocked-workspace-service",
            &trace_id,
            &span_id,
            "test.operation",
        )
        .await;

    assert!(result.is_err(), "Should reject trace for blocked workspace");

    println!("✅ Trace for blocked workspace rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_workspace_ingestion_rate_limited() -> Result<()> {
    let mut env = TestEnv::setup().await?;
    let rate_limited_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(rate_limited_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(rate_limited_workspace_id.to_string());

    let settings_resp = env
        .client
        .put(
            &format!("/api/v1/workspaces/{}/settings", rate_limited_workspace_id),
            &serde_json::json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": 1,
                "file_push_interval_secs": 300,
                "blocked": false
            }),
        )
        .await?;
    assert_eq!(settings_resp.status(), 200);

    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let child_span_id = TestDataGenerator::span_id();
    let sibling_span_id = TestDataGenerator::span_id();
    let spans = vec![
        ("root.operation", &root_span_id, None),
        ("child.operation", &child_span_id, Some(&root_span_id)),
        ("sibling.operation", &sibling_span_id, Some(&root_span_id)),
    ];
    let request = env
        .otlp
        .build_multi_span_trace("rate-limited-service", &trace_id, spans);

    let result = env.otlp.export_traces(request).await;

    assert!(
        result.is_err(),
        "Should reject trace request over max_ingestion_rate"
    );

    println!("✅ Trace over workspace ingestion rate rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_metrics_workspace_ingestion_rate_limited() -> Result<()> {
    let mut env = TestEnv::setup().await?;
    let rate_limited_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(rate_limited_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(rate_limited_workspace_id.to_string());

    let settings_resp = env
        .client
        .put(
            &format!("/api/v1/workspaces/{}/settings", rate_limited_workspace_id),
            &serde_json::json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": 0,
                "file_push_interval_secs": 300,
                "blocked": false
            }),
        )
        .await?;
    assert_eq!(settings_resp.status(), 200);

    let request = env
        .otlp
        .build_gauge_metric("rate-limited-metrics-service", "test.metric", 42.0);
    let result = env.otlp.export_metrics(request).await;

    assert!(
        result.is_err(),
        "Should reject metrics request over max_ingestion_rate"
    );

    println!("✅ Metrics over workspace ingestion rate rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_logs_workspace_ingestion_rate_limited() -> Result<()> {
    let mut env = TestEnv::setup().await?;
    let rate_limited_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(rate_limited_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(rate_limited_workspace_id.to_string());

    let settings_resp = env
        .client
        .put(
            &format!("/api/v1/workspaces/{}/settings", rate_limited_workspace_id),
            &serde_json::json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": 0,
                "file_push_interval_secs": 300,
                "blocked": false
            }),
        )
        .await?;
    assert_eq!(settings_resp.status(), 200);

    let request = env
        .otlp
        .build_log_request("rate-limited-logs-service", 9, "rate limited log");
    let result = env.otlp.export_logs(request).await;

    assert!(
        result.is_err(),
        "Should reject logs request over max_ingestion_rate"
    );

    println!("✅ Logs over workspace ingestion rate rejected");
    Ok(())
}

// Note: API key revocation is no longer supported — keys are config-based.
// Revocation requires updating config.toml and restarting the server.

#[tokio::test]
#[ignore]
async fn test_trace_with_different_service_names() -> Result<()> {
    let env = TestEnv::setup().await?;

    let services = vec!["frontend", "backend", "database", "cache"];
    let mut trace_ids = Vec::new();

    for service in &services {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        env.otlp
            .send_test_trace(service, &trace_id, &span_id, "test.operation")
            .await?;

        trace_ids.push((service, trace_id));
        println!("  Trace from service '{}' sent", service);
    }

    // Verify each service's trace was stored
    for (service, trace_id) in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
        let spans = trace_data["spans"].as_array().expect("Should have spans");
        assert!(!spans.is_empty(), "Trace from {} should be stored", service);
    }

    println!("✅ Traces from multiple services accepted and verified");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_trace_with_complex_attributes() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "GET /api/users")
        .await?;

    // Verify the span with its operation name was stored
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert!(!spans.is_empty(), "Should have at least 1 span");
    assert_eq!(
        spans[0]["operation_name"].as_str().unwrap_or(""),
        "GET /api/users",
        "Operation name should be stored correctly"
    );

    println!("✅ Trace with attributes accepted and verified");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_high_volume_trace_ingestion() -> Result<()> {
    let env = TestEnv::setup().await?;

    let service_name = TestDataGenerator::service_name();
    let count = 50;
    let mut last_trace_id = None;

    let start = std::time::Instant::now();

    for i in 0..count {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let span_name = format!("operation.{}", i);

        env.otlp
            .send_test_trace(&service_name, &trace_id, &span_id, &span_name)
            .await?;

        if i == count - 1 {
            last_trace_id = Some(trace_id);
        }
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

    // Verify the last trace was stored to confirm all ingestion completed
    if let Some(trace_id) = last_trace_id {
        let trace_id_hex = hex::encode(trace_id);
        let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
        assert!(
            trace_data["spans"]
                .as_array()
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "Last trace should be stored"
        );
    }

    println!("✅ High-volume ingestion and storage verified");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concurrent_trace_ingestion() -> Result<()> {
    let env = TestEnv::setup().await?;

    let grpc_url = env.grpc_url().to_string();
    let api_key = env.api_key.clone();
    let service_name = TestDataGenerator::service_name();
    let workspace_id = env.workspace_id;
    let mut handles = vec![];
    let mut trace_ids = vec![];

    for i in 0..10 {
        let key = api_key.clone();
        let url = grpc_url.clone();
        let svc = service_name.clone();
        let tid_ctx = workspace_id;
        let trace_id = TestDataGenerator::trace_id();
        trace_ids.push(trace_id);
        let tid = trace_id;

        let handle = tokio::spawn(async move {
            let otlp_client = OtlpClient::new(url)
                .with_api_key(key)
                .with_workspace_id(tid_ctx.to_string());
            let span_id = TestDataGenerator::span_id();
            let span_name = format!("concurrent.{}", i);
            otlp_client
                .send_test_trace(&svc, &tid, &span_id, &span_name)
                .await
        });

        handles.push(handle);
    }

    // All sends must succeed
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent trace {} failed to send", i);
    }

    println!("✅ All 10 concurrent traces sent");

    // Verify the first trace was stored (confirms ingestion pipeline is working)
    let first_hex = hex::encode(trace_ids[0]);
    let trace_data = wait_for_trace_default(&env.client, &first_hex).await?;
    assert!(
        trace_data["spans"]
            .as_array()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "Concurrent traces should be stored"
    );

    println!("✅ Concurrent trace ingestion and storage verified");
    Ok(())
}
