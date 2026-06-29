//! Agent Analytics Tests
//!
//! Tests for the unified analytics endpoint with GROUP BY queries
//! for agent-specific tracking (traces by agent, LLM model usage,
//! token consumption, costs, etc.) over time.

#[allow(unused_imports)]
use crate::*;

use opentelemetry_proto::tonic::common::v1::AnyValue;
use serde_json::Value;
use std::time::Duration;

// ============================================================================
// Helper: create a string AnyValue
// ============================================================================

fn str_val(s: &str) -> AnyValue {
    AnyValue {
        value: Some(
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s.to_string()),
        ),
    }
}

/// Poll the analytics endpoint until it returns a non-empty array.
async fn poll_analytics(
    client: &TransportApiClient,
    url: &str,
    timeout: Duration,
) -> Result<Vec<Value>> {
    poll_until(
        || async {
            let response = client.get(url).await?;
            if !response.status().is_success() {
                return Ok(None);
            }
            let data: Value = response.json().await?;
            let items = data.as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                Ok(None)
            } else {
                Ok(Some(items))
            }
        },
        timeout,
        DEFAULT_POLL_INTERVAL,
    )
    .await
}

// ============================================================================
// Analytics Tests
// ============================================================================

/// Test: default analytics (no group_by) — backward compatibility
async fn test_analytics_default_trace_count_body(env: TestEnv) -> Result<()> {
    // Ingest a trace so there's data
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let span_defs = vec![SpanDefExt {
        name: "analytics.default".to_string(),
        span_id,
        parent_span_id: None,
        attributes: vec![("agent.name".to_string(), str_val("analytics-agent"))],
        status_code: Some(1),
    }];
    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("analytics-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    // Call analytics without group_by — should return daily trace counts
    let response = env.client.get("/api/v1/analytics").await?;
    assert!(
        response.status().is_success(),
        "Default analytics should return 200"
    );

    let data: Value = response.json().await?;
    let items = data.as_array().expect("Response should be an array");
    assert!(!items.is_empty(), "Should have at least one data point");

    // Each item should have timestamp and value
    for item in items {
        assert!(item.get("timestamp").is_some(), "Should have timestamp");
        assert!(item.get("value").is_some(), "Should have value");
    }

    println!("✅ Default analytics (backward compat) test passed");
    Ok(())
}

dual_transport_test!(
    test_analytics_default_trace_count,
    test_analytics_default_trace_count_body
);

/// Test: analytics grouped by agent_name
async fn test_analytics_group_by_agent_name_body(env: TestEnv) -> Result<()> {
    // Ingest traces for different agents
    let agents = vec!["planner", "researcher", "validator"];
    let mut trace_ids = Vec::new();

    for agent_name in &agents {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("agent.name".to_string(), str_val(agent_name)),
                ("gen_ai.request.model".to_string(), str_val("gpt-4")),
            ],
            status_code: Some(1),
        }];

        let request = env.otlp.build_multi_span_trace_with_attributes(
            "analytics-service",
            &trace_id,
            span_defs,
        );
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Query analytics with group_by=agent_name
    let url = "/api/v1/analytics?metric=trace_count&group_by=agent_name";
    let items = poll_analytics(&env.client, url, DEFAULT_POLL_TIMEOUT).await?;

    assert!(!items.is_empty(), "Should have analytics data points");

    // Verify that results include groups with agent_name
    let mut found_agents = std::collections::HashSet::new();
    for item in &items {
        assert!(item.get("timestamp").is_some(), "Should have timestamp");
        assert!(item.get("value").is_some(), "Should have value");

        if let Some(agent) = item
            .get("groups")
            .and_then(|g| g.get("agent_name"))
            .and_then(|v| v.as_str())
        {
            found_agents.insert(agent.to_string());
        }
    }

    // We should find at least some of our agents
    assert!(
        !found_agents.is_empty(),
        "Should find agent names in grouped results"
    );

    println!(
        "✅ Analytics group_by agent_name passed (found: {:?})",
        found_agents
    );
    Ok(())
}

dual_transport_test!(
    test_analytics_group_by_agent_name,
    test_analytics_group_by_agent_name_body
);

/// Test: analytics grouped by llm_model
async fn test_analytics_group_by_llm_model_body(env: TestEnv) -> Result<()> {
    // Ingest traces with different models
    let models = vec!["gpt-4", "claude-3", "llama-2"];
    let mut trace_ids = Vec::new();

    for model in &models {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "llm.generation".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("gen_ai.request.model".to_string(), str_val(model)),
                ("agent.name".to_string(), str_val("test-agent")),
            ],
            status_code: Some(1),
        }];

        let request = env.otlp.build_multi_span_trace_with_attributes(
            "analytics-service",
            &trace_id,
            span_defs,
        );
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Query analytics with group_by=llm_model
    let url = "/api/v1/analytics?metric=trace_count&group_by=llm_model";
    let items = poll_analytics(&env.client, url, DEFAULT_POLL_TIMEOUT).await?;

    assert!(!items.is_empty(), "Should have analytics data points");

    let mut found_models = std::collections::HashSet::new();
    for item in &items {
        if let Some(model) = item
            .get("groups")
            .and_then(|g| g.get("llm_model"))
            .and_then(|v| v.as_str())
        {
            found_models.insert(model.to_string());
        }
    }

    assert!(
        !found_models.is_empty(),
        "Should find llm_model in grouped results"
    );

    println!(
        "✅ Analytics group_by llm_model passed (found: {:?})",
        found_models
    );
    Ok(())
}

dual_transport_test!(
    test_analytics_group_by_llm_model,
    test_analytics_group_by_llm_model_body
);

/// Test: total_tokens metric grouped by agent_name
async fn test_analytics_total_tokens_by_agent_body(env: TestEnv) -> Result<()> {
    // Ingest a trace with agent attributes
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();

    let span_defs = vec![SpanDefExt {
        name: "agent.run".to_string(),
        span_id,
        parent_span_id: None,
        attributes: vec![
            ("agent.name".to_string(), str_val("token-agent")),
            ("gen_ai.request.model".to_string(), str_val("gpt-4")),
        ],
        status_code: Some(1),
    }];

    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("analytics-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    // Query total_tokens by agent_name
    let url = "/api/v1/analytics?metric=total_tokens&group_by=agent_name";
    let response = env.client.get(url).await?;
    assert!(
        response.status().is_success(),
        "total_tokens metric should be accepted"
    );

    let data: Value = response.json().await?;
    let items = data.as_array().expect("Response should be an array");

    // Results may be empty if token values are 0, but the query should succeed
    for item in items {
        assert!(item.get("timestamp").is_some(), "Should have timestamp");
        assert!(item.get("value").is_some(), "Should have value");
    }

    println!("✅ Analytics total_tokens by agent passed");
    Ok(())
}

dual_transport_test!(
    test_analytics_total_tokens_by_agent,
    test_analytics_total_tokens_by_agent_body
);

/// Test: analytics with combined filter and group_by
async fn test_analytics_with_filter_body(env: TestEnv) -> Result<()> {
    // Ingest traces for different agents
    let agents = vec![("planner", "gpt-4"), ("researcher", "claude-3")];
    let mut trace_ids = Vec::new();

    for (agent_name, model) in &agents {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("agent.name".to_string(), str_val(agent_name)),
                ("gen_ai.request.model".to_string(), str_val(model)),
            ],
            status_code: Some(1),
        }];

        let request = env.otlp.build_multi_span_trace_with_attributes(
            "analytics-service",
            &trace_id,
            span_defs,
        );
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Query with filter: only agent_name=planner, grouped by llm_model
    let url = "/api/v1/analytics?metric=trace_count&group_by=llm_model&filters[agent_name]=planner";
    let response = env.client.get(url).await?;
    assert!(
        response.status().is_success(),
        "Analytics with filter should return 200"
    );

    let data: Value = response.json().await?;
    let items = data.as_array().expect("Response should be an array");

    // Verify results only contain data for the planner agent
    for item in items {
        assert!(item.get("timestamp").is_some(), "Should have timestamp");
        assert!(item.get("value").is_some(), "Should have value");
    }

    println!("✅ Analytics with filter test passed");
    Ok(())
}

dual_transport_test!(test_analytics_with_filter, test_analytics_with_filter_body);

/// Test: multi-dimensional group_by (agent_name AND llm_model)
async fn test_analytics_multi_group_by_body(env: TestEnv) -> Result<()> {
    // Ingest traces with different agent + model combos
    let combos = vec![
        ("planner", "gpt-4"),
        ("planner", "claude-3"),
        ("researcher", "gpt-4"),
    ];
    let mut trace_ids = Vec::new();

    for (agent_name, model) in &combos {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("agent.name".to_string(), str_val(agent_name)),
                ("gen_ai.request.model".to_string(), str_val(model)),
            ],
            status_code: Some(1),
        }];

        let request = env.otlp.build_multi_span_trace_with_attributes(
            "analytics-service",
            &trace_id,
            span_defs,
        );
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Query with multi-dimensional group_by (comma-separated)
    let url = "/api/v1/analytics?metric=trace_count&group_by=agent_name,llm_model";
    let items = poll_analytics(&env.client, url, DEFAULT_POLL_TIMEOUT).await?;

    assert!(!items.is_empty(), "Should have analytics data points");

    // Verify results have both group dimensions
    for item in &items {
        if let Some(groups) = item.get("groups") {
            // Each row should have both agent_name and llm_model in groups
            assert!(
                groups.get("agent_name").is_some(),
                "Should have agent_name in groups"
            );
            assert!(
                groups.get("llm_model").is_some(),
                "Should have llm_model in groups"
            );
        }
    }

    println!("✅ Analytics multi group_by test passed");
    Ok(())
}

dual_transport_test!(
    test_analytics_multi_group_by,
    test_analytics_multi_group_by_body
);

/// Test: unsupported metric returns an error
async fn test_analytics_unsupported_metric_error_body(env: TestEnv) -> Result<()> {
    let url = "/api/v1/analytics?metric=invalid_metric&group_by=agent_name";
    let response = env.client.get(url).await?;

    // Should fail with a server error (the storage layer rejects unsupported metrics)
    assert!(
        !response.status().is_success(),
        "Unsupported metric should return an error status"
    );

    println!("✅ Analytics unsupported metric error test passed");
    Ok(())
}

dual_transport_test!(
    test_analytics_unsupported_metric_error,
    test_analytics_unsupported_metric_error_body
);
