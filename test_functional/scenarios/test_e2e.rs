//! End-to-end integration tests

#[allow(unused_imports)]
use crate::*;
use std::collections::{HashMap, HashSet};

async fn test_complete_observability_workflow_body(env: TestEnv) -> Result<()> {
    println!("=== Complete Observability Workflow ===\n");

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    env.otlp
        .send_test_trace(
            "e2e-test-service",
            &trace_id,
            &span_id,
            "e2e.test.operation",
        )
        .await?;
    println!("Trace sent (ID: {})", format_trace_id(&trace_id));

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("trace should have spans");
    assert_eq!(spans.len(), 1, "workflow should persist exactly one span");
    let span = &spans[0];
    assert_eq!(span["trace_id"].as_str().unwrap(), trace_id_hex);
    assert_eq!(span["span_id"].as_str().unwrap(), hex::encode(span_id));
    assert_eq!(span["service_name"].as_str().unwrap(), "e2e-test-service");
    assert_eq!(
        span["operation_name"].as_str().unwrap(),
        "e2e.test.operation"
    );

    println!("\nComplete workflow successful!\n");
    Ok(())
}

dual_transport_test!(
    test_complete_observability_workflow,
    test_complete_observability_workflow_body
);

async fn test_multi_service_observability_body(env: TestEnv) -> Result<()> {
    println!("=== Multi-Service Observability ===\n");

    let services = vec![
        ("frontend", vec!["render.page", "fetch.data"]),
        ("backend-api", vec!["handle.request", "query.database"]),
        ("auth-service", vec!["verify.token"]),
    ];

    let mut expected = Vec::new();
    for (service, operations) in &services {
        for operation in operations {
            let trace_id = TestDataGenerator::trace_id();
            let span_id = TestDataGenerator::span_id();
            env.otlp
                .send_test_trace(service, &trace_id, &span_id, operation)
                .await?;
            expected.push((trace_id, service.to_string(), operation.to_string()));
        }
        println!("{} traces from '{}'", operations.len(), service);
    }

    for (trace_id, service, operation) in expected {
        let trace_id_hex = hex::encode(trace_id);
        let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
        let spans = trace_data["spans"]
            .as_array()
            .expect("trace should have spans");
        assert_eq!(spans.len(), 1, "each synthetic trace should have one span");
        assert_eq!(spans[0]["service_name"].as_str().unwrap(), service);
        assert_eq!(spans[0]["operation_name"].as_str().unwrap(), operation);
    }

    println!("\nMulti-service traces ingested successfully!\n");
    Ok(())
}

dual_transport_test!(
    test_multi_service_observability,
    test_multi_service_observability_body
);

async fn test_distributed_trace_flow_body(env: TestEnv) -> Result<()> {
    println!("=== Distributed Trace Flow ===\n");

    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let api_span_id = TestDataGenerator::span_id();
    let auth_span_id = TestDataGenerator::span_id();
    let backend_span_id = TestDataGenerator::span_id();
    let db_span_id = TestDataGenerator::span_id();

    let spans = vec![
        ("frontend.render", &root_span_id, None),
        ("api-gateway.route", &api_span_id, Some(&root_span_id)),
        ("auth.verify", &auth_span_id, Some(&api_span_id)),
        ("backend.process", &backend_span_id, Some(&api_span_id)),
        ("database.query", &db_span_id, Some(&backend_span_id)),
    ];

    let request = env
        .otlp
        .build_multi_span_trace("distributed-app", &trace_id, spans);
    env.otlp.export_traces(request).await?;

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"].as_array().expect("Should have spans");
    assert_eq!(spans.len(), 5, "Distributed trace should have 5 spans");

    let by_name: HashMap<&str, &Value> = spans
        .iter()
        .map(|span| {
            (
                span["operation_name"]
                    .as_str()
                    .expect("span should have operation_name"),
                span,
            )
        })
        .collect();
    let names: HashSet<&str> = by_name.keys().copied().collect();
    assert_eq!(
        names,
        HashSet::from([
            "frontend.render",
            "api-gateway.route",
            "auth.verify",
            "backend.process",
            "database.query",
        ])
    );

    assert_eq!(
        by_name["api-gateway.route"]["parent_span_id"].as_str(),
        Some(hex::encode(root_span_id).as_str())
    );
    assert_eq!(
        by_name["auth.verify"]["parent_span_id"].as_str(),
        Some(hex::encode(api_span_id).as_str())
    );
    assert_eq!(
        by_name["backend.process"]["parent_span_id"].as_str(),
        Some(hex::encode(api_span_id).as_str())
    );
    assert_eq!(
        by_name["database.query"]["parent_span_id"].as_str(),
        Some(hex::encode(backend_span_id).as_str())
    );
    println!("Distributed trace verified ({} spans)", spans.len());
    println!("\nDistributed trace flow simulated!\n");
    Ok(())
}

dual_transport_test!(
    test_distributed_trace_flow,
    test_distributed_trace_flow_body
);
