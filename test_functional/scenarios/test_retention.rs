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

async fn test_cleanup_deletes_all_with_zero_retention_body(mut env: TestEnv) -> Result<()> {
    // Use a dedicated workspace_id so this destructive test is idempotent in
    // reuse mode and does not affect other workspaces running in parallel.
    let retention_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(retention_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(retention_workspace_id.to_string());
    env.workspace_id = retention_workspace_id.into();

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
    // Wait until all 3 ingested traces are queryable before cleanup. Otherwise
    // a trace still in the WAL when cleanup runs would flush afterward and
    // break the "all deleted" assertion (WAL-async ingest).
    let client = &env.client;
    let url = traces_url.as_str();
    poll_until(
        || async {
            let body: serde_json::Value = client.get(url).await?.json().await?;
            let total = body["total"].as_i64().unwrap_or(0);
            Ok((total >= 3).then_some(()))
        },
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await?;

    let before_json: serde_json::Value = env.client.get(&traces_url).await?.json().await?;
    let total_before = before_json["total"].as_i64().unwrap_or(0);
    assert!(total_before > 0, "Should have traces before cleanup");

    let cleanup_resp = env
        .client
        .post(
            &format!(
                "/api/v1/admin/retention/run?retention_days=0&workspace_id={retention_workspace_id}"
            ),
            &serde_json::Value::Null,
        )
        .await?;
    assert_eq!(
        cleanup_resp.status(),
        200,
        "Cleanup endpoint should return 200"
    );

    let json = cleanup_resp.json().await?;
    let cleanup: CleanupResponse = serde_json::from_value(json)?;
    assert!(cleanup.stats.files_deleted > 0, "Should have deleted files");
    assert!(cleanup.stats.errors.is_empty(), "No errors expected");

    let after_json: serde_json::Value = env.client.get(&traces_url).await?.json().await?;
    let total_after = after_json["total"].as_i64().unwrap_or(0);
    assert_eq!(total_after, 0, "All traces should be deleted after cleanup");

    println!("Cleanup with 0-day retention deletes all data");
    Ok(())
}

dual_transport_test!(
    test_cleanup_deletes_all_with_zero_retention,
    test_cleanup_deletes_all_with_zero_retention_body
);

async fn test_cleanup_preserves_recent_data_body(mut env: TestEnv) -> Result<()> {
    // Use a dedicated workspace_id so cleanup is scoped and parallel-safe.
    let retention_workspace_id = Uuid::new_v4();

    env.client
        .set_workspace_id(retention_workspace_id.to_string());
    env.otlp = OtlpClient::new(env.ctx.config.grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string())
        .with_workspace_id(retention_workspace_id.to_string());
    env.workspace_id = retention_workspace_id.into();

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
    // Wait until BOTH recent traces are actually queryable before running
    // cleanup. A weaker "any item present" wait can let cleanup and the
    // assertion below run before the second recent trace has flushed
    // (WAL-async ingest), so assert on exactly what the test checks.
    let client = &env.client;
    let url = traces_url.as_str();
    poll_until(
        || async {
            let body: serde_json::Value = client.get(url).await?.json().await?;
            let recent = body["items"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter(|t| t["service_name"].as_str() == Some("new-service"))
                        .count()
                })
                .unwrap_or(0);
            Ok((recent >= 2).then_some(()))
        },
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await?;

    // Run cleanup with 7-day retention
    let cleanup_resp = env
        .client
        .post(
            &format!(
                "/api/v1/admin/retention/run?retention_days=7&workspace_id={retention_workspace_id}"
            ),
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

dual_transport_test!(
    test_cleanup_preserves_recent_data,
    test_cleanup_preserves_recent_data_body
);

async fn test_cleanup_requires_auth_body(_env: TestEnv) -> Result<()> {
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

dual_transport_test!(test_cleanup_requires_auth, test_cleanup_requires_auth_body);
