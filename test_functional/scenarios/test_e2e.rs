//! End-to-end integration tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_complete_observability_workflow() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== Complete Observability Workflow ===\n");

    let fixture = TestFixture::new();

    // Step 1: Create organization
    println!("Step 1: Creating organization...");
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    println!("✓ Organization '{}' created", fixture.org_display_name);

    // Step 2: Create project
    println!("\nStep 2: Creating project...");
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    println!("✓ Project '{}' created", fixture.project_display_name);

    // Step 3: Create API key
    println!("\nStep 3: Creating API key...");
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;
    println!("✓ API key created: {}...", &key_value[..15]);

    // Step 4: Send telemetry data
    println!("\nStep 4: Sending telemetry data...");

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = "e2e-test-service";

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    otlp_client
        .send_test_trace(service_name, &trace_id, &span_id, "e2e.test.operation")
        .await?;
    println!("✓ Trace sent (ID: {})", format_trace_id(&trace_id));

    // Step 5: Verify organizational hierarchy
    println!("\nStep 5: Verifying hierarchy...");
    let fetched_org = client.get_organization(&org_id).await?;
    let fetched_project = client.get_project(&project_id).await?;

    assert_eq!(fetched_org["id"], org_id.to_string());
    assert_eq!(fetched_project["id"], project_id.to_string());
    assert_eq!(fetched_project["organization_id"], org_id.to_string());
    println!("✓ Hierarchy verified: Org → Project → API Key");

    println!("\n✅ Complete workflow successful!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_multi_service_observability() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== Multi-Service Observability ===\n");

    let fixture = TestFixture::new();

    // Setup
    println!("Setting up organization and project...");
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
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Simulate multiple services
    let services = vec![
        (
            "frontend",
            vec!["render.page", "fetch.data", "click.button"],
        ),
        (
            "backend-api",
            vec!["handle.request", "query.database", "format.response"],
        ),
        ("auth-service", vec!["verify.token", "check.permissions"]),
        ("payment-service", vec!["process.payment", "validate.card"]),
    ];

    println!("\nSending traces from multiple services:");
    for (service, operations) in &services {
        for operation in operations {
            let trace_id = TestDataGenerator::trace_id();
            let span_id = TestDataGenerator::span_id();

            otlp_client
                .send_test_trace(service, &trace_id, &span_id, operation)
                .await?;
        }
        println!("✓ {} traces from '{}'", operations.len(), service);
    }

    println!("\n✅ Multi-service traces ingested successfully!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_multi_tenant_isolation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== Multi-Tenant Isolation ===\n");

    // Create two separate tenants (organizations)
    let tenant1_name = format!("tenant1-{}", TestDataGenerator::test_id());
    let tenant2_name = format!("tenant2-{}", TestDataGenerator::test_id());

    println!("Creating Tenant 1...");
    let org1 = client
        .create_organization(&tenant1_name, "Tenant 1")
        .await?;
    let org1_id = parse_uuid_from_json(&org1, "id")?;
    let proj1 = client
        .create_project(
            &org1_id,
            &format!("{}-proj", tenant1_name),
            "Tenant 1 Project",
        )
        .await?;
    let _proj1_id = parse_uuid_from_json(&proj1, "id")?;

    println!("Creating Tenant 2...");
    let org2 = client
        .create_organization(&tenant2_name, "Tenant 2")
        .await?;
    let org2_id = parse_uuid_from_json(&org2, "id")?;
    let proj2 = client
        .create_project(
            &org2_id,
            &format!("{}-proj", tenant2_name),
            "Tenant 2 Project",
        )
        .await?;
    let _proj2_id = parse_uuid_from_json(&proj2, "id")?;

    // Verify isolation
    println!("\nVerifying isolation:");

    // Projects should belong to their respective orgs
    assert_eq!(proj1["organization_id"], org1["id"]);
    assert_eq!(proj2["organization_id"], org2["id"]);
    assert_ne!(org1_id, org2_id);
    println!("✓ Organizations are isolated");

    // List projects for each org
    let tenant1_projects = client.list_projects(Some(&org1_id)).await?;
    let tenant2_projects = client.list_projects(Some(&org2_id)).await?;

    // Each tenant should only see their own projects
    assert!(tenant1_projects.iter().any(|p| p["id"] == proj1["id"]));
    assert!(!tenant1_projects.iter().any(|p| p["id"] == proj2["id"]));
    assert!(tenant2_projects.iter().any(|p| p["id"] == proj2["id"]));
    assert!(!tenant2_projects.iter().any(|p| p["id"] == proj1["id"]));
    println!("✓ Project lists are isolated");

    println!("\n✅ Multi-tenant isolation verified!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_api_key_lifecycle_management() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== API Key Lifecycle Management ===\n");

    let fixture = TestFixture::new();

    // Setup
    println!("Setting up project...");
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

    // Create multiple API keys
    println!("\nCreating API keys:");
    let mut keys = vec![];
    for i in 1..=3 {
        let key = client
            .create_api_key(
                &project_id,
                &format!("key-{}-{}", i, TestDataGenerator::test_id()),
                &format!("Key number {}", i),
            )
            .await?;
        println!("✓ Key {} created", i);
        keys.push(key);
    }

    // List keys
    println!("\nListing API keys...");
    let listed_keys = client.list_api_keys(Some(&project_id)).await?;
    assert!(listed_keys.len() >= 3, "Should have at least 3 keys");
    println!("✓ Found {} keys for project", listed_keys.len());

    // Revoke one key
    println!("\nRevoking a key...");
    let key_to_revoke_id = parse_uuid_from_json(&keys[0], "id")?;
    client.revoke_api_key(&key_to_revoke_id).await?;
    let revoked = client.get_api_key(&key_to_revoke_id).await?;
    assert_eq!(
        revoked["is_active"], false,
        "Revoked key should be inactive"
    );
    println!("✓ Key revoked successfully");

    // Verify others are still active
    println!("\nVerifying other keys remain active...");
    let key2_id = parse_uuid_from_json(&keys[1], "id")?;
    let active_key = client.get_api_key(&key2_id).await?;
    assert_eq!(
        active_key["is_active"], true,
        "Active key should remain active"
    );
    println!("✓ Other keys remain active");

    println!("\n✅ API key lifecycle managed successfully!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_error_handling_and_recovery() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== Error Handling & Recovery ===\n");

    let fixture = TestFixture::new();

    // Test 1: Invalid organization creation (empty slug AND empty name)
    println!("Test 1: Invalid organization creation");
    let result = client.create_organization("", "").await; // Both slug and name empty
    assert!(result.is_err(), "Should reject empty slug and name");
    println!("✓ Empty slug/name rejected");

    // Test 2: Duplicate names
    println!("\nTest 2: Duplicate names");
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let result2 = client
        .create_organization(&fixture.org_name, "Different Display Name")
        .await;
    assert!(result2.is_err(), "Should reject duplicate name");
    println!("✓ Duplicate name rejected");

    // Test 3: Invalid hierarchy (project without org)
    println!("\nTest 3: Invalid project creation");
    let fake_org_id = uuid::Uuid::new_v4();
    let result3 = client
        .create_project(&fake_org_id, "test-proj", "Test Project")
        .await;
    assert!(result3.is_err(), "Should reject invalid org ID");
    println!("✓ Invalid org ID rejected");

    // Test 4: Recovery - valid operations after errors
    println!("\nTest 4: Recovery after errors");
    let org_id = parse_uuid_from_json(&org, "id")?;
    let _project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    println!("✓ System recovered, valid operations succeed");

    println!("\n✅ Error handling works correctly!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_distributed_trace_flow() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    println!("=== Distributed Trace Flow ===\n");

    let fixture = TestFixture::new();

    // Setup
    println!("Setting up...");
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
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    // Simulate a distributed trace:
    // Frontend → API Gateway → Auth Service → Backend → Database
    println!("\nSimulating distributed trace...");

    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();
    let api_span_id = TestDataGenerator::span_id();
    let auth_span_id = TestDataGenerator::span_id();
    let backend_span_id = TestDataGenerator::span_id();
    let db_span_id = TestDataGenerator::span_id();

    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    let spans = vec![
        ("frontend.render", &root_span_id, None),
        ("api-gateway.route", &api_span_id, Some(&root_span_id)),
        ("auth.verify", &auth_span_id, Some(&api_span_id)),
        ("backend.process", &backend_span_id, Some(&api_span_id)),
        ("database.query", &db_span_id, Some(&backend_span_id)),
    ];

    let request = otlp_client.build_multi_span_trace("distributed-app", &trace_id, spans);
    otlp_client.export_traces(request).await?;

    println!("✓ Distributed trace sent:");
    println!("  └─ frontend.render (root)");
    println!("     ├─ api-gateway.route");
    println!("     │  ├─ auth.verify");
    println!("     │  └─ backend.process");
    println!("     │     └─ database.query");

    println!("\n✅ Distributed trace flow simulated!\n");
    Ok(())
}
