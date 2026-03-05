//! Project API tests

#[allow(unused_imports)]
use crate::*;


#[tokio::test]
#[ignore]
async fn test_create_project() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create organization first
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;

    // Create project
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;

    assert_eq!(project["slug"], fixture.project_slug());
    assert_eq!(project["name"], fixture.project_display_name);
    assert_eq!(project["organization_id"], org["id"]);
    assert_json_has_key(&project, "id")?;
    assert_json_has_key(&project, "created_at")?;

    println!("✅ Project created successfully");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_project_by_id() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org and project
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let created = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&created, "id")?;

    // Get it back
    let fetched = client.get_project(&project_id).await?;

    assert_eq!(fetched["id"], created["id"]);
    assert_eq!(fetched["slug"], fixture.project_slug());
    assert_eq!(fetched["name"], fixture.project_display_name);
    assert_eq!(fetched["organization_id"], org["id"]);

    println!("✅ Get project by ID works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_list_projects() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let created = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let created_id = parse_uuid_from_json(&created, "id")?;

    // List projects for the organization
    let projects = client.list_projects(Some(&org_id)).await?;
    assert_not_empty(&projects, "Should have at least one project")?;

    // Find our project
    let found = projects
        .iter()
        .any(|p| p["id"].as_str().unwrap_or("") == created_id.to_string());
    assert!(found, "Created project should be in list");

    println!("✅ List projects works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_list_projects_by_organization() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create organization
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;

    // Create multiple projects in this org
    let proj1 = client
        .create_project(&org_id, &format!("{}-1", fixture.project_name), "Project 1")
        .await?;
    let proj2 = client
        .create_project(&org_id, &format!("{}-2", fixture.project_name), "Project 2")
        .await?;

    // List projects for this org
    let projects = client.list_projects(Some(&org_id)).await?;

    assert!(projects.len() >= 2, "Should have at least 2 projects");

    // Both should belong to our org
    for project in &projects {
        if project["id"] == proj1["id"] || project["id"] == proj2["id"] {
            assert_eq!(project["organization_id"], org["id"]);
        }
    }

    println!("✅ List projects by organization works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_update_project() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let created = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&created, "id")?;

    // Update
    let new_display_name = format!("{} - Updated", fixture.project_display_name);
    let updated = client
        .update_project(&project_id, &new_display_name)
        .await?;

    assert_eq!(updated["name"], new_display_name);
    assert_eq!(updated["slug"], fixture.project_slug()); // Slug unchanged

    println!("✅ Project update works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_project_hierarchy_integrity() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create org and project
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

    // Verify project belongs to org
    assert_eq!(project["organization_id"], org["id"]);

    // Verify by fetching project
    let project_id = parse_uuid_from_json(&project, "id")?;
    let fetched = client.get_project(&project_id).await?;
    assert_eq!(fetched["organization_id"], org["id"]);

    println!("✅ Project-organization hierarchy maintained");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_add_project_member_by_email() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let admin_client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org and project
    let org = admin_client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = admin_client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;

    // Create a second user
    let member_email = format!("member-{}@test.com", uuid::Uuid::new_v4());
    let member_password = "SecurePassword123!";
    let register_response = admin_client
        .register(&member_email, member_password, "Test Member")
        .await?;
    assert_json_has_key(&register_response, "user")?;
    assert_json_has_key(register_response.get("user").unwrap(), "id")?;

    // Admin adds the member to the project by email
    let add_member_body = serde_json::json!({
        "user_email": member_email,
        "role": "developer"
    });

    println!("Adding member to project: {}", project_id);

    let add_response = admin_client
        .post(
            &format!("/api/v1/projects/{}/members", project_id),
            &add_member_body,
        )
        .await?;

    let status = add_response.status();
    let response_text: String = add_response.text().await?;

    println!("Add member response ({}): {}", status, response_text);

    assert_eq!(
        status, 201,
        "Expected 201 Created, got {}: {}",
        status, response_text
    );

    let member: serde_json::Value = serde_json::from_str(&response_text)?;
    assert_json_has_key(&member, "id")?;
    assert_json_has_key(&member, "user_id")?;
    assert_eq!(member["role"], "developer");

    // List project members
    let list_response = admin_client
        .get(&format!("/api/v1/projects/{}/members", project_id))
        .await?;

    assert_eq!(list_response.status(), 200);

    let members_list: serde_json::Value = list_response.json().await?;
    let members = members_list.as_array().expect("Expected array of members");

    println!(
        "Project members: {}",
        serde_json::to_string_pretty(&members_list)?
    );

    // Should have at least one member (the one we just added)
    assert!(
        !members.is_empty(),
        "Expected at least 1 member, got {}",
        members.len()
    );

    // Find our added member
    let found_member = members
        .iter()
        .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("developer"));
    assert!(found_member.is_some(), "Added member not found in list");

    // Test duplicate add returns error
    println!("Testing duplicate member add");
    let duplicate_response = admin_client
        .post(
            &format!("/api/v1/projects/{}/members", project_id),
            &add_member_body,
        )
        .await?;

    let duplicate_status = duplicate_response.status();
    println!("Duplicate add response status: {}", duplicate_status);

    // Should return 409 Conflict
    assert_eq!(
        duplicate_status, 409,
        "Expected 409 Conflict for duplicate member"
    );

    // Test invalid email returns error
    println!("Testing invalid email");
    let invalid_email_body = serde_json::json!({
        "user_email": "nonexistent@test.com",
        "role": "viewer"
    });

    let invalid_response = admin_client
        .post(
            &format!("/api/v1/projects/{}/members", project_id),
            &invalid_email_body,
        )
        .await?;

    let invalid_status = invalid_response.status();
    println!("Invalid email response status: {}", invalid_status);

    // Should return 404 Not Found
    assert_eq!(
        invalid_status, 404,
        "Expected 404 Not Found for invalid email"
    );

    println!("✅ Project member management test passed");
    Ok(())
}
