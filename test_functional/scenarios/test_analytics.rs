use functional_tests::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[allow(dead_code)]
struct AnalyticsResult {
    #[allow(dead_code)]
    timestamp: String,
    #[allow(dead_code)]
    value: f64,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct MetricsSummary {
    total_traces: i64,
    error_rate: f64,
}

#[tokio::test]
#[ignore]
async fn test_analytics_endpoints() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    // Login as admin
    let api_client = ctx.login_as_admin().await?;

    // 1. Create organization and project
    let org = api_client
        .create_organization("analytics-org", "Analytics Org")
        .await?;
    let org_id = crate::helpers::test_helpers::parse_uuid_from_json(&org, "id")?;

    let project = api_client
        .create_project(&org_id, "analytics-project", "Analytics Project")
        .await?;
    let project_id = crate::helpers::test_helpers::parse_uuid_from_json(&project, "id")?;

    // 2. Create API Key for ingestion
    let api_key_json = api_client
        .create_api_key(&project_id, "ingest-key", "Key for ingestion")
        .await?;
    let api_key = crate::helpers::test_helpers::get_string_from_json(&api_key_json, "key")?;

    // 3. Ingest traces
    let mut otlp_client = ctx.otlp_client;
    otlp_client.set_api_key(api_key.to_string());

    // Send 3 success traces
    for i in 0..3 {
        let trace_id = crate::helpers::grpc_client::random_trace_id();
        let span_id = crate::helpers::grpc_client::random_span_id();
        otlp_client
            .send_test_trace(
                "test-service",
                &trace_id,
                &span_id,
                &format!("success-span-{}", i),
            )
            .await?;
    }

    // Send 1 error trace
    // We need to manually build this one to set the error status
    let trace_id = crate::helpers::grpc_client::random_trace_id();
    let span_id = crate::helpers::grpc_client::random_span_id();
    let mut request =
        otlp_client.build_test_trace("test-service", &trace_id, &span_id, "error-span");

    // Modify status to Error (code 2)
    if let Some(res_span) = request.resource_spans.get_mut(0)
        && let Some(scope_span) = res_span.scope_spans.get_mut(0)
        && let Some(span) = scope_span.spans.get_mut(0)
    {
        span.status = Some(opentelemetry_proto::tonic::trace::v1::Status {
            message: "Something went wrong".to_string(),
            code: 2, // STATUS_CODE_ERROR
        });
        // Also update http.status_code attribute if present
        if let Some(attr) = span
            .attributes
            .iter_mut()
            .find(|kv| kv.key == "http.status_code")
        {
            attr.value = Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(500),
                ),
            });
        }
    }
    otlp_client.export_traces(request).await?;

    // Wait for ingestion (async processing)
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 4. Get daily trace counts (series)
    let series_response = api_client
        .get(&format!("/api/v1/analytics?project_id={}", project_id))
        .await?;

    assert_eq!(series_response.status(), 200);
    let series: Vec<AnalyticsResult> = series_response.json().await?;

    // We expect at least one data point for today
    assert!(!series.is_empty(), "Should have analytics data");
    let total_count: f64 = series.iter().map(|p| p.value).sum();
    assert_eq!(total_count, 4.0, "Should have 4 total traces");

    // 5. Get metrics summary
    let metrics_response = api_client
        .get(&format!(
            "/api/v1/analytics/metrics?project_id={}",
            project_id
        ))
        .await?;

    assert_eq!(metrics_response.status(), 200);
    let metrics: MetricsSummary = metrics_response.json().await?;

    assert_eq!(metrics.total_traces, 4);
    // Error rate: 1 error out of 4 traces = 0.25 (25%)
    assert_eq!(metrics.error_rate, 0.25, "Error rate should be 0.25 (25%)");

    println!("✅ Analytics endpoints work correctly with data");
    Ok(())
}
