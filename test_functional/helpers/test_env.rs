//! Shared test environment — server is ready and API key is configured.
//!
//! With config-based auth there is no login/org/project provisioning.
//! Tests use the API key from `TEST_API_KEY` environment variable (or the
//! default `zk_test_default`).

use anyhow::Result;
use uuid::Uuid;

use crate::TestContext;
use crate::helpers::{ApiClient, OtlpClient};

/// A ready-to-use test environment: server is up, API key is set.
pub struct TestEnv {
    pub ctx: TestContext,
    /// REST client with API key already set.
    pub client: ApiClient,
    /// Tenant ID — unique per test for full isolation.
    pub tenant_id: Uuid,
    /// Project ID — unique per test for full isolation.
    pub project_id: Uuid,
    /// Raw API key string.
    pub api_key: String,
    /// OTLP gRPC client pre-configured with `api_key`.
    pub otlp: OtlpClient,
}

impl TestEnv {
    /// Set up the test environment.
    ///
    /// Waits up to 30 s for the server to be ready, then returns a context
    /// with the API key from `TEST_API_KEY` (or default).
    pub async fn setup() -> Result<Self> {
        let ctx = TestContext::new();
        ctx.wait_for_ready(30).await?;

        // Each test gets a unique tenant_id and project_id for full data isolation.
        // Spawned OtlpClients must clone both to keep writes and queries aligned.
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let api_key = ctx.config.api_key.clone();
        let mut client = ApiClient::new(ctx.config.api_url.clone());
        client.set_token(api_key.clone());
        client.set_tenant_id(tenant_id.to_string());
        client.set_project_id(project_id.to_string());

        let otlp = OtlpClient::new(ctx.config.grpc_url.clone())
            .with_api_key(api_key.clone())
            .with_tenant_id(tenant_id.to_string())
            .with_project_id(project_id.to_string());

        Ok(Self {
            ctx,
            client,
            tenant_id,
            project_id,
            api_key,
            otlp,
        })
    }

    /// Convenience: return the gRPC URL from the underlying config.
    pub fn grpc_url(&self) -> &str {
        &self.ctx.config.grpc_url
    }

    /// Convenience: return the REST API URL from the underlying config.
    pub fn api_url(&self) -> &str {
        &self.ctx.config.api_url
    }
}

/// Lightweight context for tests that only need a server + authenticated client.
pub struct TestSession {
    pub ctx: TestContext,
    pub client: ApiClient,
}

impl TestSession {
    pub async fn setup() -> Result<Self> {
        let ctx = TestContext::new();
        ctx.wait_for_ready(30).await?;
        let mut client = ApiClient::new(ctx.config.api_url.clone());
        client.set_token(ctx.config.api_key.clone());
        Ok(Self { ctx, client })
    }

    /// Create a bare unauthenticated client pointed at the same server.
    pub fn unauthenticated_client(&self) -> ApiClient {
        ApiClient::new(self.ctx.config.api_url.clone())
    }
}
