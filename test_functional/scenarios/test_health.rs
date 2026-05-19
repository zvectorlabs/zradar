//! Health endpoint tests

#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_health_endpoint_returns_ok() -> Result<()> {
    let session = TestSession::setup().await?;
    let health = session.client.health().await?;

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
    let session = TestSession::setup().await?;
    let ready = session.client.health_ready().await?;

    assert_eq!(ready["ready"], true);
    assert!(ready.get("checks").is_some());

    let checks = &ready["checks"];
    assert_eq!(checks["database"], "healthy");
    assert_eq!(checks["storage"], "healthy");
    assert!(
        checks["circuit_breaker"] == "healthy" || checks["circuit_breaker"] == "degraded",
        "Circuit breaker check should report healthy or degraded"
    );
    assert_eq!(checks["retention"], "healthy");
    assert_eq!(checks["ingestion"], "healthy");
    assert_eq!(checks["background_jobs"], "healthy");

    println!("✅ Health ready checks all dependencies");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_health_live_probe() -> Result<()> {
    let session = TestSession::setup().await?;
    session.client.health_live().await?;

    println!("✅ Health live probe succeeds");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_health_endpoints_no_auth_required() -> Result<()> {
    let session = TestSession::setup().await?;
    let unauth_client = session.unauthenticated_client();

    let health = unauth_client.health().await?;
    assert_eq!(health["status"], "ok");

    let ready = unauth_client.health_ready().await?;
    assert_eq!(ready["ready"], true);

    unauth_client.health_live().await?;

    println!("✅ Health endpoints accessible without authentication");
    Ok(())
}
