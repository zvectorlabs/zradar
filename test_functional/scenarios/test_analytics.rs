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
    let env = TestEnv::setup().await?;

    // Send 3 success traces
    for i in 0..3 {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        env.otlp
            .send_test_trace(
                "test-service",
                &trace_id,
                &span_id,
                &format!("success-span-{}", i),
            )
            .await?;
    }

    // Send 1 error trace with error status
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let mut request = env
        .otlp
        .build_test_trace("test-service", &trace_id, &span_id, "error-span");
    if let Some(res_span) = request.resource_spans.get_mut(0)
        && let Some(scope_span) = res_span.scope_spans.get_mut(0)
        && let Some(span) = scope_span.spans.get_mut(0)
    {
        span.status = Some(opentelemetry_proto::tonic::trace::v1::Status {
            message: "Something went wrong".to_string(),
            code: 2, // STATUS_CODE_ERROR
        });
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
    env.otlp.export_traces(request).await?;

    // Poll until the analytics endpoint returns data for all 4 traces
    let analytics_url = "/api/v1/analytics".to_string();
    poll_until(
        || async {
            let resp = env.client.get(&analytics_url).await?;
            if resp.status() != 200 {
                return Ok(None);
            }
            let series: Vec<AnalyticsResult> = resp.json().await?;
            let total: f64 = series.iter().map(|p| p.value).sum();
            if total >= 4.0 { Ok(Some(())) } else { Ok(None) }
        },
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(200),
    )
    .await?;

    // Now fetch and verify final analytics state
    let series_response = env.client.get(&analytics_url).await?;
    assert_eq!(series_response.status(), 200);
    let series: Vec<AnalyticsResult> = series_response.json().await?;
    assert!(!series.is_empty(), "Should have analytics data");
    let total_count: f64 = series.iter().map(|p| p.value).sum();
    assert_eq!(total_count, 4.0, "Should have 4 total traces");

    // Get metrics summary
    let metrics_response = env
        .client
        .get(&format!("/api/v1/analytics/metrics?"))
        .await?;
    assert_eq!(metrics_response.status(), 200);
    let metrics: MetricsSummary = metrics_response.json().await?;
    assert_eq!(metrics.total_traces, 4);
    assert_eq!(metrics.error_rate, 0.25, "Error rate should be 0.25 (25%)");

    println!("✅ Analytics endpoints work correctly with data");
    Ok(())
}
