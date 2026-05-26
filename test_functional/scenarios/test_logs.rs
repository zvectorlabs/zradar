//! Logs Functional Tests
//!
//! Tests for OTLP logs ingestion via gRPC and querying via REST API.
//! Covers log storage, severity mapping, filtering, and auth.

#[allow(unused_imports)]
use crate::*;

// ============================================================================
// Positive Tests
// ============================================================================

/// Ingest a log entry and verify it can be retrieved via REST
#[tokio::test]
#[ignore]
async fn test_log_ingestion_and_retrieval() -> Result<()> {
    let env = TestEnv::setup().await?;

    // severity_number 9 = INFO
    let request = env
        .otlp
        .build_log_request("test-service", 9, "Hello from functional test");
    env.otlp.export_logs(request).await?;
    println!("✅ Log sent via OTLP");

    let url = "/api/v1/logs".to_string();
    let items = wait_for_items_default(&env.client, &url).await?;

    let log = &items[0];
    assert_eq!(log["service_name"].as_str().unwrap(), "test-service");
    assert_eq!(
        log["message"].as_str().unwrap(),
        "Hello from functional test"
    );

    println!("✅ Log retrieved and message/service verified");
    Ok(())
}

/// Severity number is correctly mapped to severity text
#[tokio::test]
#[ignore]
async fn test_log_severity_mapping() -> Result<()> {
    let env = TestEnv::setup().await?;

    let cases: &[(&str, i32)] = &[("DEBUG", 5), ("INFO", 9), ("WARN", 13), ("ERROR", 17)];

    for (expected_severity, severity_number) in cases {
        let msg = format!("{}-level message", expected_severity);
        let request = env
            .otlp
            .build_log_request("severity-test-svc", *severity_number, &msg);
        env.otlp.export_logs(request).await?;
    }

    // Verify each severity is stored correctly via polling
    for (expected_severity, _) in cases {
        let url = format!(
            "/api/v1/logs?service_name=severity-test-svc&severity={}",
            expected_severity
        );
        let items = wait_for_items_default(&env.client, &url).await?;
        assert!(
            !items.is_empty(),
            "Expected log with severity {}",
            expected_severity
        );
        for item in &items {
            assert_eq!(
                item["severity"].as_str().unwrap(),
                *expected_severity,
                "Severity mismatch in stored log"
            );
        }
    }

    println!("✅ Severity number → text mapping verified for DEBUG/INFO/WARN/ERROR");
    Ok(())
}

/// Filter logs by severity returns only matching entries
#[tokio::test]
#[ignore]
async fn test_log_severity_filter() -> Result<()> {
    let env = TestEnv::setup().await?;

    // INFO = 9, ERROR = 17
    env.otlp
        .export_logs(env.otlp.build_log_request("filter-svc", 9, "info message"))
        .await?;
    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("filter-svc", 17, "error message"),
        )
        .await?;

    // Poll for ERROR logs specifically
    let url = "/api/v1/logs?severity=ERROR".to_string();
    let items = wait_for_items_default(&env.client, &url).await?;

    assert!(!items.is_empty(), "Expected at least one ERROR log");
    for item in &items {
        assert_eq!(
            item["severity"].as_str().unwrap(),
            "ERROR",
            "severity filter must not return non-ERROR logs"
        );
    }

    println!("✅ severity filter works correctly");
    Ok(())
}

/// Filter logs by service_name
#[tokio::test]
#[ignore]
async fn test_log_service_name_filter() -> Result<()> {
    let env = TestEnv::setup().await?;

    env.otlp
        .export_logs(env.otlp.build_log_request("svc-alpha", 9, "from alpha"))
        .await?;
    env.otlp
        .export_logs(env.otlp.build_log_request("svc-beta", 9, "from beta"))
        .await?;

    let url = "/api/v1/logs?service_name=svc-alpha";
    let items = wait_for_items_default(&env.client, url).await?;

    for item in &items {
        assert_eq!(
            item["service_name"].as_str().unwrap(),
            "svc-alpha",
            "service_name filter must not return other services"
        );
    }

    println!("✅ service_name filter works correctly");
    Ok(())
}

/// Logs with trace_id are linked: filtering by trace_id returns only those logs
#[tokio::test]
#[ignore]
async fn test_log_trace_id_filter() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    // Log with trace_id
    let request = env.otlp.build_log_request_with_attrs(
        "traced-svc",
        9,
        "log with trace",
        &trace_id,
        &span_id,
        &[],
    );
    env.otlp.export_logs(request).await?;

    // Log without trace_id
    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("traced-svc", 9, "log without trace"),
        )
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let url = format!("/api/v1/logs?trace_id={}", trace_id_hex);
    let items = wait_for_items_default(&env.client, &url).await?;

    assert!(!items.is_empty(), "Expected log with trace_id");
    for item in &items {
        let tid = item["trace_id"].as_str().unwrap_or("");
        assert_eq!(tid, trace_id_hex, "trace_id filter returned wrong log");
    }

    println!("✅ trace_id filter works correctly");
    Ok(())
}

/// Full-text search via search_text filter
#[tokio::test]
#[ignore]
async fn test_log_search_text_filter() -> Result<()> {
    let env = TestEnv::setup().await?;

    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("search-svc", 9, "unique-needle-xyz in the log"),
        )
        .await?;
    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("search-svc", 9, "completely different message"),
        )
        .await?;

    let url = "/api/v1/logs?search_text=unique-needle-xyz";
    let items = wait_for_items_default(&env.client, url).await?;

    assert!(!items.is_empty(), "Expected to find log with search_text");
    for item in &items {
        let msg = item["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("unique-needle-xyz"),
            "search_text result should contain the search term, got: {}",
            msg
        );
    }

    println!("✅ search_text filter works correctly");
    Ok(())
}

/// Retrieve a specific log by its ID
#[tokio::test]
#[ignore]
async fn test_get_log_by_id() -> Result<()> {
    let env = TestEnv::setup().await?;

    let request = env
        .otlp
        .build_log_request("id-test-svc", 9, "fetchable log message");
    env.otlp.export_logs(request).await?;

    // Poll until the log is stored, then grab its ID
    let url = "/api/v1/logs".to_string();
    let items = wait_for_items_default(&env.client, &url).await?;
    assert!(!items.is_empty(), "Need at least one log to test get-by-id");

    let log_id = items[0]["id"].as_str().expect("log must have id");

    // Fetch by ID
    let detail_url = format!("/api/v1/logs/{}", log_id);
    let detail_response = env.client.get(&detail_url).await?;
    let detail_status = detail_response.status();
    if detail_status != 200 {
        let body = detail_response.text().await.unwrap_or_default();
        panic!(
            "Expected 200 for get-by-id, got {}: {}",
            detail_status, body
        );
    }

    let log_detail: Value = detail_response.json().await?;
    assert_eq!(log_detail["id"].as_str().unwrap(), log_id);
    assert_eq!(log_detail["service_name"].as_str().unwrap(), "id-test-svc");

    println!("✅ get-log-by-id works correctly");
    Ok(())
}

/// Logs from different projects are isolated (tenant isolation)
#[tokio::test]
#[ignore]
async fn test_logs_tenant_isolation() -> Result<()> {
    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;

    let request = env_a
        .otlp
        .build_log_request("isolated-svc", 9, "secret-log-message-proj-a");
    env_a.otlp.export_logs(request).await?;

    // Wait for project A's log to land
    let url_a = "/api/v1/logs".to_string();
    wait_for_items_default(&env_a.client, &url_a).await?;

    // Project B must not see Project A's logs
    let url_b = "/api/v1/logs".to_string();
    let response = env_b.client.get(&url_b).await?;
    assert_eq!(response.status(), 200);
    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(items.is_empty(), "Project B must not see Project A's logs");

    println!("✅ Logs tenant isolation verified");
    Ok(())
}

// ============================================================================
// Negative Tests
// ============================================================================

/// Log export without API key is rejected
#[tokio::test]
#[ignore]
async fn test_logs_export_requires_api_key() -> Result<()> {
    let session = TestSession::setup().await?;
    let no_auth = OtlpClient::new(session.ctx.config.grpc_url.clone());

    let request = no_auth.build_log_request("svc", 9, "test");
    let result = no_auth.export_logs(request).await;
    assert!(result.is_err(), "Export without API key must fail");

    println!("✅ Unauthenticated log export correctly rejected");
    Ok(())
}

/// Log export with invalid API key is rejected
#[tokio::test]
#[ignore]
async fn test_logs_export_invalid_api_key_rejected() -> Result<()> {
    let session = TestSession::setup().await?;
    let bad_client = OtlpClient::new(session.ctx.config.grpc_url.clone())
        .with_api_key("not-a-valid-api-key".to_string());

    let request = bad_client.build_log_request("svc", 9, "test");
    let result = bad_client.export_logs(request).await;
    assert!(result.is_err(), "Export with invalid API key must fail");

    println!("✅ Invalid API key correctly rejected for logs");
    Ok(())
}
