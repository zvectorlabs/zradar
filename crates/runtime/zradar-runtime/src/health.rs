//! Health check endpoints for zradar server

use api_optel::CircuitBreaker;
use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::net::SocketAddr;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

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
    pub storage: CheckStatus,
    pub circuit_breaker: CheckStatus,
    pub queue_depth: CheckStatus,
    pub retention: CheckStatus,
    pub ingestion: CheckStatus,
    pub background_jobs: CheckStatus,
}

#[derive(Debug, Clone)]
pub struct HealthState {
    pub pg_pool: Option<Arc<PgPool>>,
    pub storage_path: PathBuf,
    pub circuit_breaker: Option<Arc<CircuitBreaker>>,
    pub retention_initialized: bool,
    pub ingestion_initialized: bool,
    pub background_jobs_started: bool,
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
pub async fn readiness(state: HealthState) -> impl IntoResponse {
    let mut checks = ReadinessChecks {
        database: CheckStatus::Unhealthy,
        storage: CheckStatus::Unhealthy,
        circuit_breaker: CheckStatus::Degraded,
        queue_depth: CheckStatus::Degraded,
        retention: CheckStatus::Unhealthy,
        ingestion: CheckStatus::Unhealthy,
        background_jobs: CheckStatus::Unhealthy,
    };

    // Check PostgreSQL connection
    if let Some(pool) = state.pg_pool {
        match sqlx::query("SELECT 1").fetch_one(pool.as_ref()).await {
            Ok(_) => checks.database = CheckStatus::Healthy,
            Err(_) => checks.database = CheckStatus::Unhealthy,
        }
    }

    match tokio::fs::metadata(&state.storage_path).await {
        Ok(metadata) if metadata.is_dir() => checks.storage = CheckStatus::Healthy,
        _ => checks.storage = CheckStatus::Unhealthy,
    }

    if let Some(circuit_breaker) = state.circuit_breaker {
        let current_depth = circuit_breaker.queue_depth();
        let max_depth = circuit_breaker.max_queue_depth();
        checks.queue_depth = if current_depth > max_depth {
            CheckStatus::Degraded
        } else {
            CheckStatus::Healthy
        };

        match circuit_breaker.check().await {
            Ok(_) => checks.circuit_breaker = CheckStatus::Healthy,
            Err(_) => checks.circuit_breaker = CheckStatus::Degraded,
        }
    }

    if state.retention_initialized {
        checks.retention = CheckStatus::Healthy;
    }

    if state.ingestion_initialized {
        checks.ingestion = CheckStatus::Healthy;
    }

    if state.background_jobs_started {
        checks.background_jobs = CheckStatus::Healthy;
    }

    let ready = matches!(checks.database, CheckStatus::Healthy)
        && matches!(checks.storage, CheckStatus::Healthy)
        && !matches!(checks.circuit_breaker, CheckStatus::Unhealthy)
        && !matches!(checks.queue_depth, CheckStatus::Unhealthy)
        && matches!(checks.retention, CheckStatus::Healthy)
        && matches!(checks.ingestion, CheckStatus::Healthy)
        && matches!(checks.background_jobs, CheckStatus::Healthy);

    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(ReadinessResponse { ready, checks }))
}

/// Probe `GET {path}` on `addr` over plain HTTP/1.0 and succeed on a `2xx`.
///
/// Backs the `zradar healthcheck` subcommand, so container health checks need
/// no HTTP client (such as `curl`) in the runtime image.
pub async fn probe(addr: SocketAddr, path: &str, timeout: Duration) -> anyhow::Result<()> {
    let request = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    let status_line = tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).await?;
        anyhow::Ok(line)
    })
    .await
    .map_err(|_| anyhow::anyhow!("health probe to {addr}{path} timed out after {timeout:?}"))??;

    match parse_status_code(&status_line) {
        Some(code) if (200..300).contains(&code) => Ok(()),
        Some(code) => anyhow::bail!("health probe to {addr}{path} returned HTTP {code}"),
        None => anyhow::bail!(
            "health probe to {addr}{path} got a malformed response: {:?}",
            status_line.trim_end()
        ),
    }
}

/// Status code from an HTTP/1.x status line (`HTTP/1.1 200 OK`).
fn parse_status_code(status_line: &str) -> Option<u16> {
    let mut parts = status_line.split_whitespace();
    parts.next().filter(|v| v.starts_with("HTTP/1."))?;
    parts.next()?.parse().ok()
}

/// Create health check router
pub fn create_health_router(state: HealthState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(liveness))
        .route(
            "/health/ready",
            get({
                let state = state.clone();
                move || readiness(state.clone())
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
        let response = readiness(HealthState {
            pg_pool: None,
            storage_path: PathBuf::from("."),
            circuit_breaker: None,
            retention_initialized: true,
            ingestion_initialized: true,
            background_jobs_started: true,
        })
        .await
        .into_response();
        // Should be unhealthy without database
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn parses_status_lines() {
        assert_eq!(parse_status_code("HTTP/1.1 200 OK\r\n"), Some(200));
        assert_eq!(
            parse_status_code("HTTP/1.0 503 Service Unavailable"),
            Some(503)
        );
        assert_eq!(parse_status_code("SSH-2.0-OpenSSH"), None);
        assert_eq!(parse_status_code(""), None);
    }

    async fn serve_health() -> SocketAddr {
        let app = create_health_router(HealthState {
            pg_pool: None,
            storage_path: PathBuf::from("."),
            circuit_breaker: None,
            retention_initialized: true,
            ingestion_initialized: true,
            background_jobs_started: true,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        addr
    }

    #[tokio::test]
    async fn probe_succeeds_against_liveness() {
        let addr = serve_health().await;
        probe(addr, "/health/live", Duration::from_secs(5))
            .await
            .expect("liveness probe passes");
    }

    #[tokio::test]
    async fn probe_fails_on_non_2xx() {
        let addr = serve_health().await;
        // Not ready without a database.
        let err = probe(addr, "/health/ready", Duration::from_secs(5))
            .await
            .expect_err("readiness without a database fails");
        assert!(err.to_string().contains("503"), "{err}");
    }

    #[tokio::test]
    async fn probe_fails_when_nothing_listens() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert!(
            probe(addr, "/health/live", Duration::from_secs(5))
                .await
                .is_err()
        );
    }
}
