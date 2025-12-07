//! End-to-end tests for evaluation scores

use functional_tests::*;
use serde_json::json;
/// Test creating a score via REST API
#[tokio::test]
#[ignore]
async fn test_create_score_via_rest() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Get or create test project
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    // Create evaluation score
    let trace_id = "trace_score_test_001";
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": trace_id,
                "name": "accuracy",
                "value": 0.95,
                "data_type": "NUMERIC",
                "source": "API",
                "comment": "High accuracy score"
            }),
        )
        .await?;

    assert_eq!(response.status(), 201, "Score creation should return 201");

    let score: serde_json::Value = response.json().await?;
    assert_eq!(score["trace_id"], trace_id);
    assert_eq!(score["name"], "accuracy");
    assert_eq!(score["value"], 0.95);
    assert!(!score["id"].as_str().unwrap().is_empty());

    Ok(())
}

/// Test retrieving scores for a trace
#[tokio::test]
#[ignore]
async fn test_get_trace_scores() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_score_test_002";

    println!(
        "🔍 DEBUG: org_id={}, project_id={}, trace_id={}",
        org_id, project_id, trace_id
    );

    // Create multiple scores
    for (name, value) in [
        ("accuracy", 0.95),
        ("hallucination", 0.12),
        ("toxicity", 0.03),
    ] {
        let response = client
            .post(
                &format!("/api/v1/projects/{}/scores", project_id),
                &json!({
                    "trace_id": trace_id,
                    "name": name,
                    "value": value,
                    "data_type": "NUMERIC",
                    "source": "EVAL"
                }),
            )
            .await?;

        if response.status() == 201 {
            let score: serde_json::Value = response.json().await?;
            println!("✅ Created score: {} with id={}", name, score["id"]);
        } else {
            println!(
                "❌ Failed to create score {}: status={}",
                name,
                response.status()
            );
        }
    }

    println!(
        "🔍 Querying: GET /api/v1/projects/{}/traces/{}/scores",
        project_id, trace_id
    );

    // Query via REST API (server logs will show ClickHouse query details)
    let response = client
        .get(&format!(
            "/api/v1/projects/{}/traces/{}/scores",
            project_id, trace_id
        ))
        .await?;
    println!("🔍 API Query response status: {}", response.status());
    assert_eq!(response.status(), 200);

    let scores: Vec<serde_json::Value> = response.json().await?;
    println!("🔍 API returned {} scores", scores.len());

    if scores.is_empty() {
        println!("❌ API returned NO scores!");
        println!("❌ Check server logs above for ClickHouse query details");
    } else {
        for score in &scores {
            println!(
                "  - API Score: name={}, trace_id={}",
                score["name"].as_str().unwrap_or("?"),
                score["trace_id"].as_str().unwrap_or("?")
            );
        }
    }

    assert_eq!(scores.len(), 3, "Should have 3 scores");

    // Verify score names
    let names: Vec<&str> = scores.iter().map(|s| s["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"accuracy"));
    assert!(names.contains(&"hallucination"));
    assert!(names.contains(&"toxicity"));

    Ok(())
}

/// Test score summary aggregation
#[tokio::test]
#[ignore]
async fn test_score_summary() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_score_test_003";

    // Create 10 accuracy scores with different values
    for i in 0..10 {
        let value = 0.8 + (i as f64 * 0.01);
        client
            .post(
                &format!("/api/v1/projects/{}/scores", project_id),
                &json!({
                    "trace_id": trace_id,
                    "name": "accuracy",
                    "value": value,
                    "data_type": "NUMERIC",
                    "source": "EVAL"
                }),
            )
            .await?;
    }

    // Wait for materialized view to update
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get summary
    let response = client
        .get(&format!(
            "/api/v1/projects/{}/traces/{}/scores/summary",
            project_id, trace_id
        ))
        .await?;
    assert_eq!(response.status(), 200);

    let summary: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(summary.len(), 1, "Should have 1 score summary");

    let accuracy_summary = &summary[0];
    assert_eq!(accuracy_summary["name"], "accuracy");
    assert_eq!(accuracy_summary["count"], 10);

    let avg = accuracy_summary["avg_value"].as_f64().unwrap();
    assert!(
        avg > 0.8 && avg < 0.9,
        "Average should be between 0.8 and 0.9"
    );

    Ok(())
}

/// Test session-level scores
#[tokio::test]
#[ignore]
async fn test_session_scores() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let session_id = "session_test_001";

    // Create scores for multiple traces in the same session
    for i in 0..5 {
        let trace_id = format!("trace_session_{}", i);
        let value = 0.7 + (i as f64 * 0.05);

        client
            .post(
                &format!("/api/v1/projects/{}/scores", project_id),
                &json!({
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "name": "quality",
                    "value": value,
                    "data_type": "NUMERIC",
                    "source": "EVAL"
                }),
            )
            .await?;
    }

    // Get session scores (data should be immediately available due to OPTIMIZE TABLE FINAL)
    let response = client
        .get(&format!(
            "/api/v1/projects/{}/sessions/{}/scores",
            project_id, session_id
        ))
        .await?;
    assert_eq!(response.status(), 200);

    let scores: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(scores.len(), 5, "Should have 5 scores");

    // Verify all have the same session_id
    for score in scores {
        assert_eq!(score["session_id"], session_id);
    }

    Ok(())
}

/// Test score validation
#[tokio::test]
#[ignore]
async fn test_score_validation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    // Missing required field (trace_id) - 422 Unprocessable Entity for validation errors
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "name": "accuracy",
                "value": 0.95
            }),
        )
        .await?;
    assert_eq!(response.status(), 422, "Should reject missing trace_id");

    // Missing required field (name) - 422 Unprocessable Entity for validation errors
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": "trace_test",
                "value": 0.95
            }),
        )
        .await?;
    assert_eq!(response.status(), 422, "Should reject missing name");

    Ok(())
}

/// Test categorical scores
#[tokio::test]
#[ignore]
async fn test_categorical_score() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_categorical_001";

    // Create categorical score
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": trace_id,
                "name": "sentiment",
                "value": 0.0,  // Not used for categorical
                "data_type": "CATEGORICAL",
                "string_value": "positive",
                "source": "EVAL"
            }),
        )
        .await?;

    assert_eq!(response.status(), 201);

    let score: serde_json::Value = response.json().await?;
    assert_eq!(score["data_type"], "CATEGORICAL");
    assert_eq!(score["string_value"], "positive");

    Ok(())
}

/// Test score deletion
#[tokio::test]
#[ignore]
async fn test_delete_score() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_delete_test_001";

    // Create score
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": trace_id,
                "name": "accuracy",
                "value": 0.95,
                "data_type": "NUMERIC",
                "source": "API"
            }),
        )
        .await?;

    println!("🔍 Create score response status: {}", response.status());
    assert_eq!(response.status(), 201, "Score creation should return 201");

    let score: serde_json::Value = response.json().await?;
    let score_id = score["id"].as_str().unwrap().to_string();

    println!("🔍 Created score with id: {}", score_id);
    println!(
        "🔍 Delete URL: /api/v1/projects/{}/scores/{}",
        project_id, score_id
    );

    // Delete score (data should be immediately available due to insert_scores in test mode)
    let response = client
        .delete(&format!(
            "/api/v1/projects/{}/scores/{}",
            project_id, &score_id
        ))
        .await?;
    println!("🔍 Delete response status: {}", response.status());
    assert_eq!(response.status(), 204, "Delete should return 204");

    // Verify score is no longer returned
    let response = client
        .get(&format!(
            "/api/v1/projects/{}/scores/{}",
            project_id, &score_id
        ))
        .await?;
    assert_eq!(response.status(), 404, "Deleted score should return 404");

    Ok(())
}

/// Test tenant isolation
#[tokio::test]
#[ignore]
async fn test_tenant_isolation() -> Result<()> {
    let ctx1 = TestContext::new();
    ctx1.wait_for_ready(30).await?;
    let client1 = ctx1.login_as_admin().await?;

    let ctx2 = TestContext::new();
    let client2 = ctx2.login_as_admin().await?;

    // Setup tenant 1
    let fixture1 = TestFixture::new();
    let org1 = client1
        .create_organization(&fixture1.org_name, &fixture1.org_display_name)
        .await?;
    let org_id1 = parse_uuid_from_json(&org1, "id")?;
    let project1 = client1
        .create_project(
            &org_id1,
            &fixture1.project_name,
            &fixture1.project_display_name,
        )
        .await?;
    let project_id1 = parse_uuid_from_json(&project1, "id")?;

    // Setup tenant 2
    let fixture2 = TestFixture::new();
    let org2 = client2
        .create_organization(&fixture2.org_name, &fixture2.org_display_name)
        .await?;
    let org_id2 = parse_uuid_from_json(&org2, "id")?;
    let project2 = client2
        .create_project(
            &org_id2,
            &fixture2.project_name,
            &fixture2.project_display_name,
        )
        .await?;
    let project_id2 = parse_uuid_from_json(&project2, "id")?;

    let trace_id = "trace_isolation_001";

    // Create score in tenant 1
    let response = client1
        .post(
            &format!("/api/v1/projects/{}/scores", project_id1),
            &json!({
                "trace_id": trace_id,
                "name": "accuracy",
                "value": 0.95,
                "data_type": "NUMERIC",
                "source": "API"
            }),
        )
        .await?;

    println!("🔍 Create score response status: {}", response.status());
    assert_eq!(response.status(), 201, "Score creation should return 201");

    // Small wait for ClickHouse (should be immediate with OPTIMIZE TABLE FINAL)
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify tenant 1 CAN see the score
    let response = client1
        .get(&format!(
            "/api/v1/projects/{}/traces/{}/scores",
            project_id1, trace_id
        ))
        .await?;
    assert_eq!(response.status(), 200);
    let tenant1_scores: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(
        tenant1_scores.len(),
        1,
        "Tenant 1 should see their own score"
    );

    // Verify tenant 2 CANNOT see tenant 1's score (tenant isolation)
    let response = client2
        .get(&format!(
            "/api/v1/projects/{}/traces/{}/scores",
            project_id2, trace_id
        ))
        .await?;
    let scores: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(scores.len(), 0, "Tenant 2 should not see tenant 1's scores");

    Ok(())
}

/// Test score with span_id
#[tokio::test]
#[ignore]
async fn test_span_score() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_span_001";
    let span_id = "span_001";

    // Create score for specific span
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": trace_id,
                "span_id": span_id,
                "name": "step_accuracy",
                "value": 0.92,
                "data_type": "NUMERIC",
                "source": "EVAL"
            }),
        )
        .await?;

    assert_eq!(response.status(), 201);

    let score: serde_json::Value = response.json().await?;
    assert_eq!(score["span_id"], span_id);

    Ok(())
}

/// Test multiple score sources
#[tokio::test]
#[ignore]
async fn test_score_sources() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_sources_001";

    // Create scores from different sources
    for source in ["API", "ANNOTATION", "EVAL"] {
        client
            .post(
                &format!("/api/v1/projects/{}/scores", project_id),
                &json!({
                    "trace_id": trace_id,
                    "name": "quality",
                    "value": 0.8,
                    "data_type": "NUMERIC",
                    "source": source
                }),
            )
            .await?;
    }

    // Get all scores (data should be immediately available due to OPTIMIZE TABLE FINAL)
    let response = client
        .get(&format!(
            "/api/v1/projects/{}/traces/{}/scores",
            project_id, trace_id
        ))
        .await?;
    let scores: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(scores.len(), 3);

    let sources: Vec<&str> = scores
        .iter()
        .map(|s| s["source"].as_str().unwrap())
        .collect();
    assert!(sources.contains(&"API"));
    assert!(sources.contains(&"ANNOTATION"));
    assert!(sources.contains(&"EVAL"));

    Ok(())
}

/// Test score with metadata
#[tokio::test]
#[ignore]
async fn test_score_with_metadata() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_metadata_001";

    // Create score with metadata
    let response = client
        .post(
            &format!("/api/v1/projects/{}/scores", project_id),
            &json!({
                "trace_id": trace_id,
                "name": "accuracy",
                "value": 0.95,
                "data_type": "NUMERIC",
                "source": "API",
                "metadata": {
                    "eval_version": "v2.0",
                    "model": "gpt-4",
                    "threshold": 0.8
                }
            }),
        )
        .await?;

    assert_eq!(response.status(), 201);

    let score: serde_json::Value = response.json().await?;
    assert!(score.get("id").is_some());

    Ok(())
}

/// Test bulk score creation performance
#[tokio::test]
#[ignore]
async fn test_bulk_score_creation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Setup
    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    let trace_id = "trace_bulk_001";
    let start = std::time::Instant::now();

    // Create 100 scores
    for i in 0..100 {
        client
            .post(
                &format!("/api/v1/projects/{}/scores", project_id),
                &json!({
                    "trace_id": trace_id,
                    "name": "accuracy",
                    "value": 0.8 + (i as f64 * 0.001)
                }),
            )
            .await?;
    }

    let duration = start.elapsed();

    // Should complete in reasonable time (adjust threshold as needed)
    assert!(
        duration.as_secs() < 30,
        "Bulk creation took too long: {:?}",
        duration
    );

    println!("Created 100 scores in {:?}", duration);

    Ok(())
}
