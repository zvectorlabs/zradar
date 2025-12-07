//! Organization API tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_create_organization() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;

    assert_eq!(org["slug"], fixture.org_slug());
    assert_eq!(org["name"], fixture.org_display_name);
    assert_json_has_key(&org, "id")?;
    assert_json_has_key(&org, "created_at")?;
    assert_json_has_key(&org, "updated_at")?;

    println!("✅ Organization created successfully");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_organization_by_id() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create organization
    let created = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&created, "id")?;

    // Get it back
    let fetched = client.get_organization(&org_id).await?;

    assert_eq!(fetched["id"], created["id"]);
    assert_eq!(fetched["slug"], fixture.org_slug());
    assert_eq!(fetched["name"], fixture.org_display_name);

    println!("✅ Get organization by ID works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_list_organizations() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create an organization
    let created = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let created_id = parse_uuid_from_json(&created, "id")?;

    // List all organizations
    let orgs = client.list_organizations().await?;

    println!("🔍 Created org ID: {}", created_id);
    println!("🔍 Number of orgs returned: {}", orgs.len());
    for (i, org) in orgs.iter().enumerate() {
        println!(
            "🔍 Org {}: Full JSON = {}",
            i,
            serde_json::to_string_pretty(org).unwrap_or_else(|_| "error".to_string())
        );
        if i >= 2 {
            break;
        } // Only print first 3
    }

    assert_not_empty(&orgs, "Should have at least one organization")?;

    // Find our created organization
    let found = orgs
        .iter()
        .any(|org| org["organization"]["id"].as_str().unwrap_or("") == created_id.to_string());

    assert!(found, "Created organization should be in list");

    println!("✅ List organizations returns created org");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_update_organization() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create organization
    let created = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&created, "id")?;

    // Update it
    let new_display_name = format!("{} - Updated", fixture.org_display_name);
    let updated = client
        .update_organization(&org_id, &new_display_name)
        .await?;

    assert_eq!(updated["name"], new_display_name);
    assert_eq!(updated["slug"], fixture.org_slug()); // Slug shouldn't change

    // Verify by fetching again
    let fetched = client.get_organization(&org_id).await?;
    assert_eq!(fetched["name"], new_display_name);

    println!("✅ Organization update works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_duplicate_organization_name_rejected() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create first organization
    let first_result = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await;
    assert!(first_result.is_ok(), "First org should be created");

    // Try to create another with same name
    let second_result = client
        .create_organization(&fixture.org_name, "Different Display Name")
        .await;
    assert!(
        second_result.is_err(),
        "Duplicate org name should be rejected"
    );

    println!("✅ Duplicate organization name rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_organization_name_validation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Empty name should fail
    let result1 = client.create_organization("", "Display Name").await;
    assert!(result1.is_err(), "Empty name should be rejected");

    // Very long name should fail (assuming there's a limit)
    let long_name = "a".repeat(500);
    let result2 = client.create_organization(&long_name, "Display Name").await;
    assert!(result2.is_err(), "Overly long name should be rejected");

    println!("✅ Organization name validation works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_organization_requires_authentication() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let unauth_client = ApiClient::new(ctx.config.api_url.clone());

    // Try to create org without auth
    let result = unauth_client
        .create_organization("test-org", "Test Org")
        .await;
    assert!(result.is_err(), "Should require authentication");

    println!("✅ Organization endpoints require authentication");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_organization_members_list_includes_user_details() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create organization
    let created_org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&created_org, "id")?;

    // Register a new user to add as member
    let new_user_email = TestDataGenerator::email();
    let new_user_password = TestDataGenerator::password();
    let new_user_name = "Test Member User";

    // We need a separate client to register the new user, or just use the admin client if registration is open
    // Using admin client to register
    let _new_user = client
        .register(&new_user_email, &new_user_password, new_user_name)
        .await?;

    // Add the new user to the organization
    client
        .add_organization_member(&org_id, &new_user_email, "member")
        .await?;

    // List members
    let members = client.list_organization_members(&org_id).await?;

    // Find the added member
    let member = members
        .iter()
        .find(|m| m.get("user_email").and_then(|e| e.as_str()) == Some(&new_user_email));

    assert!(member.is_some(), "Added member should be in the list");
    let member = member.unwrap();

    // Verify user details are present
    assert_eq!(member["user_email"], new_user_email);
    assert_eq!(member["user_full_name"], new_user_name);
    assert_eq!(member["role"], "member");

    println!("✅ Organization member list includes user details");
    Ok(())
}
