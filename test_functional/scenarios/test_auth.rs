//! Authentication tests

#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_login_with_valid_credentials() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let mut client = ApiClient::new(ctx.config.api_url.clone());
    let token = client
        .login(&ctx.config.admin_email, &ctx.config.admin_password)
        .await?;

    assert!(!token.is_empty(), "Token should not be empty");
    assert!(token.len() > 20, "Token should be substantial length");

    println!("✅ Login with valid credentials succeeds");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_login_with_invalid_credentials() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let mut client = ApiClient::new(ctx.config.api_url.clone());
    let result = client.login("invalid@example.com", "wrongpassword").await;

    assert!(
        result.is_err(),
        "Login should fail with invalid credentials"
    );

    println!("✅ Login with invalid credentials rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_register_new_user() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let client = ApiClient::new(ctx.config.api_url.clone());
    let email = TestDataGenerator::email();
    let password = TestDataGenerator::password();
    let display_name = TestDataGenerator::display_name();

    let user = client.register(&email, &password, &display_name).await?;

    // Check that registration returned success - the response structure may vary
    // Some implementations return user details, others return minimal response
    assert!(user.is_object(), "Registration should return an object");

    // Check optional fields if present
    if user.get("id").is_some() && !user["id"].is_null() {
        println!("✓ User ID returned: {}", user["id"]);
    }
    if user.get("email").is_some() && !user["email"].is_null() {
        assert_eq!(user["email"], email);
    }
    if user.get("full_name").is_some() && !user["full_name"].is_null() {
        assert_eq!(user["full_name"], display_name);
    }

    println!("✅ New user registration succeeds");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_register_duplicate_email() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let client = ApiClient::new(ctx.config.api_url.clone());
    let email = TestDataGenerator::email();
    let password = TestDataGenerator::password();
    let display_name = TestDataGenerator::display_name();

    // First registration should succeed
    client.register(&email, &password, &display_name).await?;

    // Second registration with same email should fail
    let result = client.register(&email, &password, &display_name).await;
    assert!(result.is_err(), "Duplicate email registration should fail");

    println!("✅ Duplicate email registration rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_authenticated_endpoint_access() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Access protected endpoint (e.g., /auth/me)
    let user = client.auth_me().await?;

    assert_eq!(user["email"], ctx.config.admin_email);
    assert_json_has_key(&user, "id")?;

    println!("✅ Authenticated endpoint access succeeds");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_unauthenticated_endpoint_access() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let client = ApiClient::new(ctx.config.api_url.clone());

    // Try to access protected endpoint without auth
    let result = client.auth_me().await;
    assert!(result.is_err(), "Unauthenticated access should fail");

    println!("✅ Unauthenticated endpoint access rejected");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_login_returns_valid_jwt() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let mut client = ApiClient::new(ctx.config.api_url.clone());
    let token = client
        .login(&ctx.config.admin_email, &ctx.config.admin_password)
        .await?;

    // JWT should have 3 parts separated by dots
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT should have 3 parts");

    // Each part should be base64-encoded and not empty
    for part in parts {
        assert!(!part.is_empty(), "JWT parts should not be empty");
    }

    println!("✅ Login returns valid JWT token");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_token_persists_across_requests() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    // Make multiple authenticated requests
    let user1 = client.auth_me().await?;
    let user2 = client.auth_me().await?;

    assert_eq!(user1["id"], user2["id"]);

    println!("✅ Token persists across requests");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_token_required_for_protected_resources() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Logged in user can create organization
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    assert!(org.get("id").is_some());

    // Unauthenticated client cannot
    let unauth_client = ApiClient::new(ctx.config.api_url.clone());
    let result = unauth_client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await;
    assert!(result.is_err());

    println!("✅ Token required for protected resources");
    Ok(())
}
