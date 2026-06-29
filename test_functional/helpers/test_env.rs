//! Shared test environment — server is ready and API key is configured.
//!
//! With config-based auth there is no login/org/workspace provisioning.
//! Tests use the API key from `TEST_API_KEY` environment variable (or the
//! default `zk_test_default`).

use anyhow::Result;
use uuid::Uuid;
use zradar_models::WorkspaceId;

use crate::TestContext;
use crate::helpers::{OtlpClient, QueryTransportClient, Transport, TransportApiClient};

/// A ready-to-use test environment: server is up, API key is set.
pub struct TestEnv {
    pub ctx: TestContext,
    /// Query/admin API client (HTTP or gRPC depending on setup).
    pub client: TransportApiClient,
    /// Normalized query helpers (same transport as `client`).
    pub query: QueryTransportClient,
    pub transport: Transport,
    pub workspace_id: WorkspaceId,
    pub api_key: String,
    pub otlp: OtlpClient,
}

impl TestEnv {
    pub async fn setup() -> Result<Self> {
        Self::setup_with_transport(Transport::Http).await
    }

    pub async fn setup_with_transport(transport: Transport) -> Result<Self> {
        let ctx = TestContext::new();
        ctx.wait_for_ready(30).await?;

        let workspace_id = Uuid::new_v4();
        let workspace_id_str = workspace_id.to_string();
        let api_key = ctx.config.api_key.clone();

        let client =
            TransportApiClient::from_test_context(&ctx, transport, &api_key, &workspace_id_str);
        let query =
            QueryTransportClient::from_test_context(&ctx, transport, &api_key, &workspace_id_str);

        let otlp = OtlpClient::new(ctx.config.grpc_url.clone())
            .with_api_key(api_key.clone())
            .with_workspace_id(workspace_id_str);

        Ok(Self {
            ctx,
            client,
            query,
            transport,
            workspace_id: workspace_id.into(),
            api_key,
            otlp,
        })
    }

    pub fn sync_workspace_id(&mut self, workspace_id: Uuid) {
        let workspace_id_str = workspace_id.to_string();
        self.client.set_workspace_id(workspace_id_str.clone());
        self.query = QueryTransportClient::from_test_context(
            &self.ctx,
            self.transport,
            &self.api_key,
            &workspace_id_str,
        );
        self.workspace_id = workspace_id.into();
    }

    pub fn grpc_url(&self) -> &str {
        &self.ctx.config.grpc_url
    }

    pub fn api_url(&self) -> &str {
        &self.ctx.config.api_url
    }
}

/// Lightweight context for tests that only need a server + authenticated client.
pub struct TestSession {
    pub ctx: TestContext,
    pub client: TransportApiClient,
}

impl TestSession {
    pub async fn setup() -> Result<Self> {
        let ctx = TestContext::new();
        ctx.wait_for_ready(30).await?;
        let mut http = crate::helpers::ApiClient::new(ctx.config.api_url.clone());
        http.set_token(ctx.config.api_key.clone());
        Ok(Self {
            ctx,
            client: http.into_transport(),
        })
    }

    pub fn unauthenticated_client(&self) -> TransportApiClient {
        crate::helpers::ApiClient::new(self.ctx.config.api_url.clone()).into_transport()
    }
}
