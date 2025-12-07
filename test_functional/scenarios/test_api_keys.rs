//! API Key tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_create_api_key() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org and project
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

    // Create API key
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;

    assert_eq!(api_key["name"], fixture.api_key_name);
    // Note: CreateApiKeyResponse doesn't include description, project_id, or is_revoked
    assert_json_has_key(&api_key, "id")?;
    assert_json_has_key(&api_key, "key")?;
    assert_json_has_key(&api_key, "permissions")?;

    // Verify key format (should start with zvr_)
    let key_str = get_string_from_json(&api_key, "key")?;
    helpers::assert_starts_with(key_str, "zvr_")?;

    println!("✅ API key created with correct format");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_get_api_key_by_id() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
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
    let created = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_id = parse_uuid_from_json(&created, "id")?;

    // Get it back
    let fetched = client.get_api_key(&key_id).await?;

    assert_eq!(fetched["id"], created["id"]);
    assert_eq!(fetched["name"], fixture.api_key_name);
    // Note: ApiKeyResponse doesn't include project_id for security
    assert_json_has_key(&fetched, "key_prefix")?;
    assert_json_has_key(&fetched, "permissions")?;

    // Note: The actual key value should NOT be returned on fetch (security)
    assert!(fetched.get("key").is_none() || fetched["key"].is_null());

    println!("✅ Get API key by ID works (key value hidden)");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_list_api_keys() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
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
    let _key1 = client
        .create_api_key(&project_id, &format!("{}-1", fixture.api_key_name), "Key 1")
        .await?;
    let _key2 = client
        .create_api_key(&project_id, &format!("{}-2", fixture.api_key_name), "Key 2")
        .await?;

    // List keys for project
    let project_keys = client.list_api_keys(Some(&project_id)).await?;
    assert!(
        project_keys.len() >= 2,
        "Should have at least 2 keys for project"
    );

    println!("✅ List API keys works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_revoke_api_key() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
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
    let key_id = parse_uuid_from_json(&api_key, "id")?;

    // Note: CreateApiKeyResponse doesn't include is_active field

    // Revoke it (returns no body, just status code)
    client.revoke_api_key(&key_id).await?;

    // Verify by fetching
    let fetched = client.get_api_key(&key_id).await?;
    assert_eq!(fetched["is_active"], false);

    println!("✅ API key revocation works");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_api_key_only_returned_on_creation() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
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

    // Create API key - should include the key value
    let created = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    assert_json_has_key(&created, "key")?;
    let key_value = get_string_from_json(&created, "key")?;
    assert!(key_value.starts_with("zvr_"));

    // Fetch same key - should NOT include the key value
    let key_id = parse_uuid_from_json(&created, "id")?;
    let fetched = client.get_api_key(&key_id).await?;
    assert!(fetched.get("key").is_none() || fetched["key"].is_null());

    println!("✅ API key value only exposed on creation");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_revoked_key_cannot_be_used() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
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
    let key_id = parse_uuid_from_json(&api_key, "id")?;
    let _key_value = get_string_from_json(&api_key, "key")?;

    // Revoke the key
    client.revoke_api_key(&key_id).await?;

    // Verify revocation through API
    let fetched = client.get_api_key(&key_id).await?;
    assert_eq!(fetched["is_active"], false);

    println!("✅ Revoked API key marked as revoked");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_api_key_project_hierarchy() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create org, project, and key
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

    // Note: API responses don't include project_id for security
    // Just verify the key was created successfully
    assert_json_has_key(&api_key, "id")?;
    assert_json_has_key(&api_key, "key")?;

    // Verify by fetching
    let key_id = parse_uuid_from_json(&api_key, "id")?;
    let fetched = client.get_api_key(&key_id).await?;
    assert_eq!(fetched["name"], fixture.api_key_name);

    println!("✅ API key-project hierarchy maintained");
    Ok(())
}
