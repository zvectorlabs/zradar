//! Health endpoint tests

use functional_tests::*;
#[tokio::test]
#[ignore]
async fn test_health_endpoint_returns_ok() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let health = ctx.api_client.health().await?;

    assert_eq!(health["status"], "ok");
    assert!(
        health.get("version").is_some(),
        "Health response should include version"
    );

    println!("✅ Health endpoint returns OK");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_health_ready_checks_dependencies() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    let ready = ctx.api_client.health_ready().await?;

    assert_eq!(ready["ready"], true);
    assert!(ready.get("checks").is_some());

    let checks = &ready["checks"];
    assert_eq!(checks["database"], "healthy");
    assert_eq!(checks["clickhouse"], "healthy");

    println!("✅ Health ready checks all dependencies");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_health_live_probe() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    ctx.api_client.health_live().await?;

    println!("✅ Health live probe succeeds");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_health_endpoints_no_auth_required() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;

    // Create client without authentication
    let unauth_client = ApiClient::new(ctx.config.api_url.clone());

    // All health endpoints should work without auth
    let health = unauth_client.health().await?;
    assert_eq!(health["status"], "ok");

    let ready = unauth_client.health_ready().await?;
    assert_eq!(ready["ready"], true);

    unauth_client.health_live().await?;

    println!("✅ Health endpoints accessible without authentication");
    Ok(())
}
