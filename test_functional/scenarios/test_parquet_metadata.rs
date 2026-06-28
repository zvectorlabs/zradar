//! Parquet metadata functional tests
//!
//! These tests verify that OTLP telemetry is written to Parquet data files and
//! registered in PostgreSQL metadata tables.

use crate::helpers::DbClient;
#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_parquet_metadata_written_for_all_signals() -> Result<()> {
    let env = TestEnv::setup().await?;
    let db = DbClient::from_env().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env.otlp
        .send_test_trace(
            "metadata-trace-svc",
            &trace_id,
            &span_id,
            "metadata.trace.operation",
        )
        .await?;

    env.otlp
        .export_metrics(env.otlp.build_gauge_metric(
            "metadata-metric-svc",
            "metadata.cpu.usage",
            12.5,
        ))
        .await?;

    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("metadata-log-svc", 9, "metadata log message"),
        )
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;
    wait_for_items_default(
        &env.client,
        "/api/v1/metrics?metric_name=metadata.cpu.usage",
    )
    .await?;
    wait_for_items_default(&env.client, "/api/v1/logs?service_name=metadata-log-svc").await?;

    for signal_type in ["traces", "metrics", "logs"] {
        let file_entries = poll_until(
            || async {
                let rows = db.file_list_entries(&env.workspace_id, signal_type).await?;
                if rows.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(rows))
                }
            },
            DEFAULT_POLL_TIMEOUT,
            DEFAULT_POLL_INTERVAL,
        )
        .await?;

        assert!(
            file_entries.iter().any(|entry| !entry.deleted),
            "{signal_type} should have at least one active file_list row"
        );

        for entry in file_entries.iter().filter(|entry| !entry.deleted) {
            assert_eq!(entry.workspace_id, env.workspace_id);
            assert_eq!(entry.workspace_id, env.workspace_id);
            assert_eq!(entry.signal_type, signal_type);
            assert_eq!(entry.location, "local");
            assert!(
                entry.file_path.ends_with(".parquet"),
                "file path should point to a parquet file: {}",
                entry.file_path
            );
            assert!(entry.records > 0, "records should be positive");
            assert!(entry.original_size > 0, "original_size should be positive");
            assert!(
                entry.compressed_size > 0,
                "compressed_size should be positive"
            );
            assert!(entry.min_ts <= entry.max_ts, "time range should be valid");
        }

        let stats_entries = db.stream_stats(&env.workspace_id, signal_type).await?;
        assert!(
            !stats_entries.is_empty(),
            "{signal_type} should have stream_stats rows"
        );
        assert!(
            stats_entries.iter().any(|entry| entry.file_count > 0),
            "{signal_type} stream_stats should count files"
        );
        assert!(
            stats_entries.iter().any(|entry| entry.total_records > 0),
            "{signal_type} stream_stats should count records"
        );
    }

    println!("✅ Parquet file_list and stream_stats verified for all signals");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_all_signal_workspace_isolation() -> Result<()> {
    let env_a = TestEnv::setup().await?;
    let env_b = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env_a
        .otlp
        .send_test_trace(
            "all-signal-isolated-trace-svc",
            &trace_id,
            &span_id,
            "all.signal.trace",
        )
        .await?;

    env_a
        .otlp
        .export_metrics(env_a.otlp.build_gauge_metric(
            "all-signal-isolated-metric-svc",
            "all.signal.metric",
            99.0,
        ))
        .await?;

    env_a
        .otlp
        .export_logs(env_a.otlp.build_log_request(
            "all-signal-isolated-log-svc",
            9,
            "all signal secret log",
        ))
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env_a.client, &trace_id_hex).await?;
    wait_for_items_default(
        &env_a.client,
        "/api/v1/metrics?metric_name=all.signal.metric",
    )
    .await?;
    wait_for_items_default(
        &env_a.client,
        "/api/v1/logs?service_name=all-signal-isolated-log-svc",
    )
    .await?;

    let trace_response = env_b
        .client
        .get(&format!("/api/v1/traces/{}", trace_id_hex))
        .await?;
    assert!(
        trace_response.status().as_u16() == 404 || trace_response.status().as_u16() == 200,
        "trace detail for another workspace should return 404 or empty 200"
    );
    if trace_response.status().is_success() {
        let trace_body: Value = trace_response.json().await?;
        let spans = trace_body
            .get("spans")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            spans.is_empty(),
            "Project B must not see Project A trace spans"
        );
    }

    let metrics_response = env_b
        .client
        .get("/api/v1/metrics?metric_name=all.signal.metric")
        .await?;
    assert_eq!(metrics_response.status(), 200);
    let metrics_body: Value = metrics_response.json().await?;
    assert!(
        metrics_body["items"].as_array().unwrap().is_empty(),
        "Project B must not see Project A metrics"
    );

    let logs_response = env_b
        .client
        .get("/api/v1/logs?service_name=all-signal-isolated-log-svc")
        .await?;
    assert_eq!(logs_response.status(), 200);
    let logs_body: Value = logs_response.json().await?;
    assert!(
        logs_body["items"].as_array().unwrap().is_empty(),
        "Project B must not see Project A logs"
    );

    println!("✅ All-signal workspace isolation verified");
    Ok(())
}
