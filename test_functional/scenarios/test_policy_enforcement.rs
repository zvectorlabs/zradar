//! Policy enforcement functional tests

#[allow(unused_imports)]
use crate::*;

fn assert_grpc_status(error: &anyhow::Error, code: tonic::Code, message: &str) -> Result<()> {
    let status = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
        .ok_or_else(|| anyhow::anyhow!("expected tonic status error, got: {error:#}"))?;

    assert_eq!(status.code(), code);
    assert!(
        status.message().contains(message),
        "expected gRPC message to contain {message:?}, got {:?}",
        status.message()
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_policy_rate_block_rejects_trace_ingest() -> Result<()> {
    let env = TestEnv::setup().await?;

    let response = env
        .client
        .put(
            "/api/v1/admin/policies/config",
            &serde_json::json!({
                "tenant_id": env.tenant_id,
                "policies": [{
                    "project_id": env.project_id,
                    "signal": "traces",
                    "operation": "ingest",
                    "limit": {
                        "kind": "rate",
                        "records_per_sec": 0,
                        "bytes_per_sec": null
                    },
                    "grace_pct": 101,
                    "hard_block_pct": 103
                }]
            }),
        )
        .await?;
    assert_eq!(response.status(), 204);

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let result = env
        .otlp
        .send_test_trace(
            "policy-rate-block-service",
            &trace_id,
            &span_id,
            "policy.blocked.operation",
        )
        .await;

    let error = result.expect_err("policy rate block should reject trace ingestion");
    assert_grpc_status(
        &error,
        tonic::Code::ResourceExhausted,
        "rate_records_exceeded",
    )?;

    println!("✅ Policy rate block rejected trace ingestion");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_usage_tracking_drives_ingest_quota_block() -> Result<()> {
    let env = TestEnv::setup().await?;

    let response = env
        .client
        .put(
            "/api/v1/admin/policies/config",
            &serde_json::json!({
                "tenant_id": env.tenant_id,
                "policies": [{
                    "project_id": env.project_id,
                    "signal": "traces",
                    "operation": "ingest",
                    "limit": {
                        "kind": "quota",
                        "max_bytes": 1,
                        "period_start": 0,
                        "period_end": null,
                        "basis": "compressed_bytes"
                    },
                    "grace_pct": 101,
                    "hard_block_pct": 103
                }]
            }),
        )
        .await?;
    assert_eq!(response.status(), 204);

    let first_trace_id = TestDataGenerator::trace_id();
    let first_span_id = TestDataGenerator::span_id();
    env.otlp
        .send_test_trace(
            "policy-usage-quota-service",
            &first_trace_id,
            &first_span_id,
            "policy.usage.first",
        )
        .await?;
    wait_for_trace_default(&env.client, &hex::encode(first_trace_id)).await?;

    let second_trace_id = TestDataGenerator::trace_id();
    let second_span_id = TestDataGenerator::span_id();
    let result = env
        .otlp
        .send_test_trace(
            "policy-usage-quota-service",
            &second_trace_id,
            &second_span_id,
            "policy.usage.second",
        )
        .await;

    let error = result.expect_err("hot usage counter should block second trace ingestion");
    assert_grpc_status(&error, tonic::Code::ResourceExhausted, "quota_exceeded")?;

    println!("✅ Hot usage counter drove quota-based trace ingestion block");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_policy_query_window_rejects_trace_query() -> Result<()> {
    let env = TestEnv::setup().await?;

    let response = env
        .client
        .put(
            "/api/v1/admin/policies/config",
            &serde_json::json!({
                "tenant_id": env.tenant_id,
                "policies": [{
                    "project_id": env.project_id,
                    "signal": "traces",
                    "operation": "query",
                    "limit": {
                        "kind": "window",
                        "max_query_days": 1
                    },
                    "grace_pct": 101,
                    "hard_block_pct": 103
                }]
            }),
        )
        .await?;
    assert_eq!(response.status(), 204);

    let valid_start_time = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let valid_end_time = chrono::Utc::now().to_rfc3339();
    let valid_response = env
        .client
        .get(&format!(
            "/api/v1/traces?start_time={}&end_time={}",
            urlencoding::encode(&valid_start_time),
            urlencoding::encode(&valid_end_time)
        ))
        .await?;
    assert_eq!(
        valid_response.status(),
        200,
        "query window policy should allow an in-window trace query"
    );

    let start_time = (chrono::Utc::now() - chrono::Duration::days(3)).to_rfc3339();
    let end_time = chrono::Utc::now().to_rfc3339();
    let response = env
        .client
        .get(&format!(
            "/api/v1/traces?start_time={}&end_time={}",
            urlencoding::encode(&start_time),
            urlencoding::encode(&end_time)
        ))
        .await?;

    assert_eq!(response.status(), 400);
    let body: serde_json::Value = response.json().await?;
    assert_eq!(
        body["error"].as_str(),
        Some("query_window_violation"),
        "query window policy should reject over-wide trace query"
    );

    println!("✅ Policy query window rejected over-wide trace query");
    Ok(())
}
