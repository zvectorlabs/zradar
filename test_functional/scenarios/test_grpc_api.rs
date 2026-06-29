//! Full gRPC API surface E2E — Query (:8081), Analytics (:8081), Admin (:8082).

use crate::*;

use api::grpc::admin_proto::PolicyConfig;

async fn poll_query<T, F, Fut>(query: ZradarQueryClient, mut check: F) -> Result<T>
where
    F: FnMut(ZradarQueryClient) -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    poll_until(
        || check(query.clone()),
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await
}

#[tokio::test]
#[ignore]
async fn test_grpc_full_api_surface() -> Result<()> {
    let env = TestEnv::setup().await?;
    let grpc = ZradarGrpcClients::from_test_env(
        env.ctx.config.query_grpc_url.clone(),
        env.ctx.config.admin_grpc_url.clone(),
        env.api_key.clone(),
        env.workspace_id.to_string(),
    );

    // ── Stage 1: OTLP ingest (traces, logs, metrics) ─────────────────────
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let operation = "grpc.full.e2e.operation";
    let trace_id_hex = hex::encode(trace_id);
    let span_id_hex = hex::encode(span_id);

    env.otlp
        .send_test_trace("grpc-full-e2e", &trace_id, &span_id, operation)
        .await?;

    env.otlp
        .export_logs(
            env.otlp
                .build_log_request("grpc-full-e2e", 9, "grpc full e2e log message"),
        )
        .await?;

    env.otlp
        .export_metrics(
            env.otlp
                .build_gauge_metric("grpc-full-e2e", "grpc.e2e.gauge", 7.5),
        )
        .await?;

    // ── Stage 2: QueryService (8 RPCs) ───────────────────────────────────
    let trace = poll_query(grpc.query.clone(), |q| {
        let trace_id_hex = trace_id_hex.clone();
        async move {
            match q.get_trace(&trace_id_hex).await {
                Ok(resp) if resp.trace.as_ref().is_some_and(|t| !t.spans.is_empty()) => {
                    Ok(Some(resp))
                }
                Ok(_) => Ok(None),
                Err(err) if grpc_not_ready(&err) => Ok(None),
                Err(err) => Err(err),
            }
        }
    })
    .await?;
    assert_eq!(
        trace.trace.as_ref().unwrap().spans[0].operation_name,
        operation
    );

    let traces = poll_query(grpc.query.clone(), |q| {
        let op = operation.to_string();
        async move {
            let resp = q.query_traces(Some(&op), None).await?;
            if resp.items.is_empty() {
                Ok(None)
            } else {
                Ok(Some(resp))
            }
        }
    })
    .await?;
    assert!(!traces.items.is_empty());

    let spans = poll_query(grpc.query.clone(), |q| {
        let trace_id_hex = trace_id_hex.clone();
        async move {
            let resp = q.query_spans(&trace_id_hex).await?;
            if resp.items.is_empty() {
                Ok(None)
            } else {
                Ok(Some(resp))
            }
        }
    })
    .await?;
    let span_id_from_query = spans.items[0].span_id.clone();

    let span = poll_query(grpc.query.clone(), |q| {
        let sid = span_id_from_query.clone();
        async move {
            match q.get_span(&sid).await {
                Ok(resp) if resp.span.is_some() => Ok(Some(resp)),
                Ok(_) => Ok(None),
                Err(err) if grpc_not_ready(&err) => Ok(None),
                Err(err) => Err(err),
            }
        }
    })
    .await?;
    assert_eq!(span.span.as_ref().unwrap().span_id, span_id_hex);

    let logs = poll_query(grpc.query.clone(), |q| async move {
        let resp = q.query_logs().await?;
        if resp.items.is_empty() {
            Ok(None)
        } else {
            Ok(Some(resp))
        }
    })
    .await?;
    let log_id = logs.items[0].id.clone();
    let log = grpc.query.get_log(&log_id).await?;
    assert!(log.log.is_some());

    let metrics = poll_query(grpc.query.clone(), |q| async move {
        let resp = q.query_metrics("grpc.e2e.gauge").await?;
        if resp.items.is_empty() {
            Ok(None)
        } else {
            Ok(Some(resp))
        }
    })
    .await?;
    assert_eq!(metrics.items[0].metric_name, "grpc.e2e.gauge");

    let _series = grpc.query.query_metric_series("grpc.e2e.gauge").await?;

    // ── Stage 3: AnalyticsService (12 RPCs) ──────────────────────────────
    let analytics = poll_query(grpc.query.clone(), |q| async move {
        let resp = q.get_analytics().await?;
        let total: f64 = resp.results.iter().map(|r| r.value).sum();
        if total >= 1.0 {
            Ok(Some(resp))
        } else {
            Ok(None)
        }
    })
    .await?;
    assert!(!analytics.results.is_empty());

    let summary = poll_query(grpc.query.clone(), |q| async move {
        let resp = q.get_metrics_summary().await?;
        if resp.total_traces >= 1 {
            Ok(Some(resp))
        } else {
            Ok(None)
        }
    })
    .await?;
    assert!(summary.total_traces >= 1);

    let _ = grpc.query.get_top_endpoints().await?;
    let _ = grpc.query.get_error_breakdown().await?;
    let _ = grpc.query.get_llm_analytics().await?;
    let _ = grpc.query.get_agent_analytics().await?;
    let _ = grpc.query.get_guardrails_analytics().await?;
    let _ = grpc.query.get_storage_usage().await?;
    let _ = grpc.query.get_storage_usage_daily().await?;
    let _ = grpc.query.get_quota_status().await?;
    let _ = grpc.query.get_usage_daily().await?;
    let _ = grpc.query.get_ingest_rate().await?;
    let _ = grpc.query.get_query_usage().await?;

    // ── Stage 4: Admin RetentionService (3 RPCs) ─────────────────────────
    let retention = grpc.admin.get_workspace_retention().await?;
    assert!(retention.retention_days > 0);

    let updated_retention = grpc.admin.set_workspace_retention(45).await?;
    assert_eq!(updated_retention.retention_days, 45);

    let cleanup = grpc.admin.run_cleanup().await?;
    assert!(cleanup.stats.is_some());

    // ── Stage 5: Admin PolicyService (3 RPCs) ──────────────────────────
    grpc.admin
        .upsert_policies(vec![PolicyConfig {
            signal: "traces".to_string(),
            operation: "ingest".to_string(),
            limit_json: r#"{"kind":"rate","records_per_sec":10000,"bytes_per_sec":null}"#
                .to_string(),
            grace_pct: Some(101),
            hard_block_pct: Some(103),
            effective_from: None,
            effective_until: None,
            source: Some("api".to_string()),
        }])
        .await?;

    let policies = grpc.admin.list_policies().await?;
    assert!(!policies.policies.is_empty());

    let effective = grpc.admin.get_effective_policy().await?;
    assert!(effective.ingest.is_some());
    assert!(effective.query.is_some());
    assert!(effective.store.is_some());

    // ── Stage 6: Admin SettingsService (2 RPCs) ────────────────────────
    let settings = grpc.admin.get_workspace_settings().await?;
    assert!(settings.settings.is_some());

    let updated = grpc
        .admin
        .update_workspace_settings(WorkspaceSettingsInput {
            traces_retention_days: 60,
            metrics_retention_days: 30,
            logs_retention_days: 30,
            max_ingestion_rate: None,
            file_push_interval_secs: 300,
            blocked: false,
            capture_llm_content_enabled: false,
        })
        .await?;
    assert_eq!(
        updated
            .settings
            .as_ref()
            .map(|s| s.traces_retention_days)
            .unwrap_or(0),
        60
    );

    // ── Stage 7: Admin AuditService (1 RPC) ────────────────────────────
    let audit = poll_until(
        || async {
            let resp = grpc
                .admin
                .list_audit_logs(Some("workspace_settings.update"))
                .await?;
            if resp.items.is_empty() {
                Ok(None)
            } else {
                Ok(Some(resp))
            }
        },
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await?;
    assert!(
        audit
            .items
            .iter()
            .any(|entry| entry.action == "workspace_settings.update")
    );

    println!("✅ Full gRPC API surface E2E passed (Query + Analytics + Admin)");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_grpc_get_trace_after_ingest() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env.otlp
        .send_test_trace(
            "grpc-query-test",
            &trace_id,
            &span_id,
            "grpc.test.operation",
        )
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    let query = ZradarQueryClient::new(env.ctx.config.query_grpc_url.clone())
        .with_api_key(env.api_key.clone())
        .with_workspace_id(env.workspace_id.to_string());

    let response = poll_until(
        || async {
            match query.get_trace(&trace_id_hex).await {
                Ok(response) => {
                    let span_count = response
                        .trace
                        .as_ref()
                        .map(|trace| trace.spans.len())
                        .unwrap_or(0);
                    if span_count == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(response))
                    }
                }
                Err(err) if grpc_not_ready(&err) => Ok(None),
                Err(err) => Err(err),
            }
        },
        DEFAULT_POLL_TIMEOUT,
        DEFAULT_POLL_INTERVAL,
    )
    .await?;

    let trace = response.trace.expect("GetTrace should return trace detail");
    assert_eq!(trace.trace_id, trace_id_hex);
    assert_eq!(trace.spans.len(), 1);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_grpc_query_requires_auth() -> Result<()> {
    let env = TestEnv::setup().await?;

    let unauthenticated = ZradarQueryClient::new(env.ctx.config.query_grpc_url.clone())
        .with_workspace_id(env.workspace_id.to_string());

    let result = unauthenticated.get_trace("00").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_grpc_admin_requires_auth() -> Result<()> {
    let env = TestEnv::setup().await?;

    let unauthenticated = ZradarAdminClient::new(env.ctx.config.admin_grpc_url.clone())
        .with_workspace_id(env.workspace_id.to_string());

    let result = unauthenticated.get_workspace_retention().await;
    assert!(result.is_err());

    Ok(())
}
