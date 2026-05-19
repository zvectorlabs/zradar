//! End-to-end integration tests

#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_complete_observability_workflow() -> Result<()> {
    let env = TestEnv::setup().await?;

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
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    println!("\nComplete workflow successful!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_multi_service_observability() -> Result<()> {
    let env = TestEnv::setup().await?;

    println!("=== Multi-Service Observability ===\n");

    let services = vec![
        ("frontend", vec!["render.page", "fetch.data"]),
        ("backend-api", vec!["handle.request", "query.database"]),
        ("auth-service", vec!["verify.token"]),
    ];

    let mut last_trace_id = None;
    for (service, operations) in &services {
        for operation in operations {
            let trace_id = TestDataGenerator::trace_id();
            let span_id = TestDataGenerator::span_id();
            env.otlp
                .send_test_trace(service, &trace_id, &span_id, operation)
                .await?;
            last_trace_id = Some(trace_id);
        }
        println!("{} traces from '{}'", operations.len(), service);
    }

    if let Some(trace_id) = last_trace_id {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    println!("\nMulti-service traces ingested successfully!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_distributed_trace_flow() -> Result<()> {
    let env = TestEnv::setup().await?;

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
    assert!(
        spans.len() >= 5,
        "Distributed trace should have at least 5 spans"
    );

    println!("Distributed trace verified ({} spans)", spans.len());
    println!("\nDistributed trace flow simulated!\n");
    Ok(())
}
