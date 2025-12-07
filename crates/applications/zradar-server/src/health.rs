//! Health check endpoints for zradar server

use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Readiness check response
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub checks: ReadinessChecks,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadinessChecks {
    pub database: CheckStatus,
    pub clickhouse: CheckStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Simple health check - always returns OK if server is running
pub async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Liveness probe - for Kubernetes/orchestrators
pub async fn liveness() -> impl IntoResponse {
    // Server is alive if it can respond
    StatusCode::OK
}

/// Readiness probe - checks dependencies
pub async fn readiness(pg_pool: Option<Arc<PgPool>>) -> impl IntoResponse {
    let mut checks = ReadinessChecks {
        database: CheckStatus::Unhealthy,
        clickhouse: CheckStatus::Healthy, // TODO: Actually check ClickHouse
    };

    // Check PostgreSQL connection
    if let Some(pool) = pg_pool {
        match sqlx::query("SELECT 1").fetch_one(pool.as_ref()).await {
            Ok(_) => checks.database = CheckStatus::Healthy,
            Err(_) => checks.database = CheckStatus::Unhealthy,
        }
    }

    let ready = matches!(checks.database, CheckStatus::Healthy)
        && matches!(checks.clickhouse, CheckStatus::Healthy);

    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(ReadinessResponse { ready, checks }))
}

/// Create health check router
pub fn create_health_router(pg_pool: Option<Arc<PgPool>>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(liveness))
        .route(
            "/health/ready",
            get({
                let pool = pg_pool.clone();
                move || readiness(pool.clone())
            }),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check() {
        let response = health().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_liveness() {
        let response = liveness().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readiness_no_pool() {
        let response = readiness(None).await.into_response();
        // Should be unhealthy without database
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
