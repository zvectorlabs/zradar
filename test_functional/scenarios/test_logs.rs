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

    // severity_number 9 = INFO
    let request = otlp.build_log_request("test-service", 9, "Hello from functional test");
    otlp.export_logs(request).await?;
    println!("✅ Log sent via OTLP");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/logs?project_id={}", project_id);
    let response = client.get(&url).await?;
    let status = response.status();
    if status != 200 {
        let body = response.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, body);
    }

    let data: Value = response.json().await?;
    let items = data["items"].as_array().expect("items must be an array");
    assert!(!items.is_empty(), "Expected at least one log");

    let log = &items[0];
    assert_eq!(log["service_name"].as_str().unwrap(), "test-service");
    assert_eq!(log["message"].as_str().unwrap(), "Hello from functional test");

    println!("✅ Log retrieved and message/service verified");
    Ok(())
}

/// Severity number is correctly mapped to severity text
#[tokio::test]
#[ignore]
async fn test_log_severity_mapping() -> Result<()> {
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

    let cases: &[(&str, i32)] = &[
        ("DEBUG", 5),
        ("INFO", 9),
        ("WARN", 13),
        ("ERROR", 17),
    ];

    for (expected_severity, severity_number) in cases {
        let msg = format!("{}-level message", expected_severity);
        let request = otlp.build_log_request("severity-test-svc", *severity_number, &msg);
        otlp.export_logs(request).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify each severity is stored correctly
    for (expected_severity, _) in cases {
        let url = format!(
            "/api/v1/logs?project_id={}&service_name=severity-test-svc&severity={}",
            project_id, expected_severity
        );
        let response = client.get(&url).await?;
        assert_eq!(response.status(), 200, "severity {} query failed", expected_severity);
        let data: Value = response.json().await?;
        let items = data["items"].as_array().unwrap();
        assert!(
            !items.is_empty(),
            "Expected log with severity {}", expected_severity
        );
        for item in items {
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

    // INFO = 9, ERROR = 17
    otlp.export_logs(otlp.build_log_request("filter-svc", 9, "info message")).await?;
    otlp.export_logs(otlp.build_log_request("filter-svc", 17, "error message")).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query only ERROR logs
    let url = format!("/api/v1/logs?project_id={}&severity=ERROR", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(!items.is_empty(), "Expected at least one ERROR log");
    for item in items {
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

    otlp.export_logs(otlp.build_log_request("svc-alpha", 9, "from alpha")).await?;
    otlp.export_logs(otlp.build_log_request("svc-beta", 9, "from beta")).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/logs?project_id={}&service_name=svc-alpha", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    for item in items {
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

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    // Log with trace_id
    let request = otlp.build_log_request_with_attrs(
        "traced-svc",
        9,
        "log with trace",
        &trace_id,
        &span_id,
        &[],
    );
    otlp.export_logs(request).await?;

    // Log without trace_id
    otlp.export_logs(otlp.build_log_request("traced-svc", 9, "log without trace")).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let trace_id_hex = hex::encode(trace_id);
    let url = format!("/api/v1/logs?project_id={}&trace_id={}", project_id, trace_id_hex);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(!items.is_empty(), "Expected log with trace_id");

    for item in items {
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

    otlp.export_logs(otlp.build_log_request("search-svc", 9, "unique-needle-xyz in the log")).await?;
    otlp.export_logs(otlp.build_log_request("search-svc", 9, "completely different message")).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("/api/v1/logs?project_id={}&search_text=unique-needle-xyz", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);

    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(!items.is_empty(), "Expected to find log with search_text");
    for item in items {
        let msg = item["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("unique-needle-xyz"),
            "search_text result should contain the search term, got: {}", msg
        );
    }

    println!("✅ search_text filter works correctly");
    Ok(())
}

/// Retrieve a specific log by its ID
#[tokio::test]
#[ignore]
async fn test_get_log_by_id() -> Result<()> {
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

    let request = otlp.build_log_request("id-test-svc", 9, "fetchable log message");
    otlp.export_logs(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // List logs to get an ID
    let url = format!("/api/v1/logs?project_id={}", project_id);
    let response = client.get(&url).await?;
    assert_eq!(response.status(), 200);
    let data: Value = response.json().await?;
    let items = data["items"].as_array().unwrap();
    assert!(!items.is_empty(), "Need at least one log to test get-by-id");

    let log_id = items[0]["id"].as_str().expect("log must have id");

    // Fetch by ID
    let detail_url = format!("/api/v1/logs/{}?project_id={}", log_id, project_id);
    let detail_response = client.get(&detail_url).await?;
    let detail_status = detail_response.status();
    if detail_status != 200 {
        let body = detail_response.text().await.unwrap_or_default();
        panic!("Expected 200 for get-by-id, got {}: {}", detail_status, body);
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
    let request = otlp_a.build_log_request("isolated-svc", 9, "secret-log-message-proj-a");
    otlp_a.export_logs(request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Project B must not see Project A's logs
    let url = format!("/api/v1/logs?project_id={}", proj_b_id);
    let response = client.get(&url).await?;
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
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client_no_auth = OtlpClient::new(ctx.config.grpc_url.clone());

    let request = client_no_auth.build_log_request("svc", 9, "test");
    let result = client_no_auth.export_logs(request).await;
    assert!(result.is_err(), "Export without API key must fail");

    println!("✅ Unauthenticated log export correctly rejected");
    Ok(())
}

/// Log export with invalid API key is rejected
#[tokio::test]
#[ignore]
async fn test_logs_export_invalid_api_key_rejected() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let bad_client = OtlpClient::new(ctx.config.grpc_url.clone())
        .with_api_key("not-a-valid-api-key".to_string());

    let request = bad_client.build_log_request("svc", 9, "test");
    let result = bad_client.export_logs(request).await;
    assert!(result.is_err(), "Export with invalid API key must fail");

    println!("✅ Invalid API key correctly rejected for logs");
    Ok(())
}

/// Querying logs without project_id returns 400/422
#[tokio::test]
#[ignore]
async fn test_logs_query_requires_project_id() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let response = client.get("/api/v1/logs").await?;
    let status = response.status().as_u16();
    assert!(
        status == 400 || status == 422,
        "Expected 400 or 422 when project_id is missing, got {}",
        status
    );

    println!("✅ Logs query without project_id correctly rejected");
    Ok(())
}
