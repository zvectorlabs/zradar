use functional_tests::*;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn days_ago_ns(days: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    now.saturating_sub(days * 86_400 * 1_000_000_000)
}

#[derive(Deserialize)]
struct CleanupResponse {
    stats: CleanupStats,
}

#[derive(Deserialize)]
struct CleanupStats {
    files_deleted: u64,
    #[allow(dead_code)]
    bytes_freed: u64,
    errors: Vec<String>,
}

#[derive(Deserialize)]
struct RetentionConfigResponse {
    org_id: Uuid,
    default_days: u32,
}

#[tokio::test]
#[ignore]
async fn test_cleanup_deletes_all_with_zero_retention() -> Result<()> {
    // Use dedicated tenant_id and project_id for this destructive test
    // to avoid interfering with other tests running in parallel
    let mut env = TestEnv::setup().await?;
    let retention_tenant_id = Uuid::new_v4();
    let retention_project_id = Uuid::new_v4();

    // Override the clients with dedicated IDs
    env.client.set_tenant_id(retention_tenant_id.to_string());
    env.client.set_project_id(retention_project_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_tenant_id(retention_tenant_id.to_string())
        .with_project_id(retention_project_id.to_string());
    env.project_id = retention_project_id;

    for i in 0..3 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        env.otlp
            .send_test_trace(
                "retention-service",
                &trace_id,
                &span_id,
                &format!("span-{}", i),
            )
            .await?;
    }

    let traces_url = "/api/v1/traces".to_string();
    wait_for_items_default(&env.client, &traces_url).await?;

    let before_json: serde_json::Value = env.client.get(&traces_url).await?.json().await?;
    let total_before = before_json["total"].as_i64().unwrap_or(0);
    assert!(total_before > 0, "Should have traces before cleanup");

    let cleanup_resp = env
        .client
        .post(
            "/api/v1/admin/retention/run?retention_days=0",
            &serde_json::Value::Null,
        )
        .await?;
    assert_eq!(
        cleanup_resp.status(),
        200,
        "Cleanup endpoint should return 200"
    );

    let cleanup: CleanupResponse = cleanup_resp.json().await?;
    assert!(cleanup.stats.files_deleted > 0, "Should have deleted files");
    assert!(cleanup.stats.errors.is_empty(), "No errors expected");

    let after_json: serde_json::Value = env.client.get(&traces_url).await?.json().await?;
    let total_after = after_json["total"].as_i64().unwrap_or(0);
    assert_eq!(total_after, 0, "All traces should be deleted after cleanup");

    println!("Cleanup with 0-day retention deletes all data");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_cleanup_preserves_recent_data() -> Result<()> {
    // Use dedicated tenant_id and project_id for this test
    // to avoid interfering with other tests running in parallel
    let mut env = TestEnv::setup().await?;
    let retention_tenant_id = Uuid::new_v4();
    let retention_project_id = Uuid::new_v4();

    // Override the clients with dedicated IDs
    env.client.set_tenant_id(retention_tenant_id.to_string());
    env.client.set_project_id(retention_project_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_tenant_id(retention_tenant_id.to_string())
        .with_project_id(retention_project_id.to_string());
    env.project_id = retention_project_id;

    // Create old data (15 days ago - should be deleted with 7-day retention)
    let old_start_ns = days_ago_ns(15);
    for i in 0..2 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let request = env.otlp.build_test_trace_with_timestamp(
            "old-service",
            &trace_id,
            &span_id,
            &format!("old-span-{}", i),
            old_start_ns,
            old_start_ns + 1_000_000_000,
        );
        env.otlp.export_traces(request).await?;
    }

    // Create recent data (should be preserved)
    for i in 0..2 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        env.otlp
            .send_test_trace(
                "new-service",
                &trace_id,
                &span_id,
                &format!("new-span-{}", i),
            )
            .await?;
    }

    let traces_url = "/api/v1/traces".to_string();
    wait_for_items_default(&env.client, &traces_url).await?;

    // Run cleanup with 7-day retention
    let cleanup_resp = env
        .client
        .post(
            "/api/v1/admin/retention/run?retention_days=7",
            &serde_json::Value::Null,
        )
        .await?;
    assert_eq!(cleanup_resp.status(), 200);

    // Verify recent data is preserved (should have 2 traces from "new-service")
    let after_json: serde_json::Value = env.client.get(&traces_url).await?.json().await?;
    let total_after = after_json["total"].as_i64().unwrap_or(0);
    assert!(
        total_after >= 2,
        "Expected at least 2 recent traces to be preserved, got {}",
        total_after
    );

    println!(
        "✅ Cleanup with 7-day retention preserved {} recent traces",
        total_after
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_set_retention_config() -> Result<()> {
    let env = TestEnv::setup().await?;
    let org_id = env.tenant_id;

    let set_resp = env
        .client
        .put(
            "/api/v1/admin/retention/config",
            &serde_json::json!({
                "org_id": org_id,
                "default_days": 14,
                "project_overrides": {}
            }),
        )
        .await?;
    assert_eq!(
        set_resp.status(),
        200,
        "Retention config update should return 200"
    );
    let set_config: RetentionConfigResponse = set_resp.json().await?;
    assert_eq!(set_config.org_id, org_id);
    assert_eq!(set_config.default_days, 14);

    let get_resp = env
        .client
        .get(&format!("/api/v1/admin/retention/config/{}", org_id))
        .await?;
    assert_eq!(
        get_resp.status(),
        200,
        "Retention config get should return 200"
    );
    let get_config: RetentionConfigResponse = get_resp.json().await?;
    assert_eq!(get_config.org_id, org_id);
    assert_eq!(get_config.default_days, 14);

    println!("Retention config set via API");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_cleanup_requires_auth() -> Result<()> {
    let session = TestSession::setup().await?;
    let unauthenticated = ApiClient::new(session.ctx.config.api_url.clone());
    let resp = unauthenticated
        .post(
            "/api/v1/admin/retention/run?retention_days=0",
            &serde_json::Value::Null,
        )
        .await?;

    let status = resp.status();
    assert!(
        status == 401 || status == 403,
        "Unauthenticated cleanup should be rejected (got {})",
        status
    );

    println!("Cleanup endpoint requires authentication");
    Ok(())
}
