//! Functional test library for zradar
//!
//! Provides utilities for black-box API testing through public endpoints only.

#![recursion_limit = "256"]
#![allow(unused_mut)] // dual_transport_test! macro may not use all env parameters

pub mod helpers;

pub use helpers::test_helpers::{
    assert_json_eq, assert_json_has_key, assert_not_empty, assert_starts_with, format_span_id,
    format_trace_id, generate_test_id, get_bool_from_json, get_i64_from_json, get_string_from_json,
    parse_uuid_from_json, wait_for_server,
};
pub use helpers::{
    ApiClient, OtlpClient, SpanDefExt, TestDataGenerator, TestFixture, TransportApiClient,
};
pub use helpers::{
    DEFAULT_POLL_INTERVAL, DEFAULT_POLL_TIMEOUT, poll_until, wait_for_items,
    wait_for_items_default, wait_for_trace, wait_for_trace_default,
};
pub use helpers::{
    ErrorBreakdownView, QueryTransportClient, SpanFilters, SpanView, TraceView, Transport,
};
pub use helpers::{TestEnv, TestSession};
pub use helpers::{
    WorkspaceSettingsInput, ZradarAdminClient, ZradarGrpcClients, ZradarQueryClient, grpc_not_ready,
};

pub use anyhow::Context;
pub use anyhow::Result;
pub use hex;
pub use serde_json::{Value, json};
pub use std::time::Duration;

/// Test configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub api_url: String,
    pub grpc_url: String,
    pub query_grpc_url: String,
    pub admin_grpc_url: String,
    /// API key configured in the server's `config.toml` `[[api_keys]]` section.
    pub api_key: String,
}

impl TestConfig {
    /// Load test configuration from environment variables with defaults.
    pub fn from_env() -> Self {
        Self {
            api_url: std::env::var("TEST_API_URL")
                .unwrap_or_else(|_| "http://localhost:9015".to_string()),
            grpc_url: std::env::var("TEST_GRPC_URL")
                .unwrap_or_else(|_| "http://localhost:9016".to_string()),
            query_grpc_url: std::env::var("TEST_QUERY_GRPC_URL")
                .unwrap_or_else(|_| "http://localhost:9017".to_string()),
            admin_grpc_url: std::env::var("TEST_ADMIN_GRPC_URL")
                .unwrap_or_else(|_| "http://localhost:9018".to_string()),
            api_key: std::env::var("TEST_API_KEY")
                .unwrap_or_else(|_| "zk_test_default".to_string()),
        }
    }
}

/// Test context shared across tests.
pub struct TestContext {
    pub config: TestConfig,
    pub api_client: ApiClient,
    pub otlp_client: OtlpClient,
}

impl TestContext {
    /// Create a new test context with the configured API key.
    pub fn new() -> Self {
        let config = TestConfig::from_env();
        let mut api_client = ApiClient::new(config.api_url.clone());
        api_client.set_token(config.api_key.clone());
        let otlp_client = OtlpClient::new(config.grpc_url.clone());

        Self {
            config,
            api_client,
            otlp_client,
        }
    }

    /// Return an authenticated client (API key already set in `new()`).
    pub fn authenticated_client(&self) -> &ApiClient {
        &self.api_client
    }

    /// Wait for server to be ready.
    pub async fn wait_for_ready(&self, timeout_secs: u64) -> Result<()> {
        wait_for_server(&self.config.api_url, timeout_secs).await
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}
