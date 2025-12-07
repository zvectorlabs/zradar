//! Functional test library for zradar
//!
//! This library provides utilities for black-box API testing of zradar services.
//! All tests verify behavior through public API endpoints only, without direct
//! database access.

pub mod helpers;

// Re-export main helpers for convenience
pub use helpers::test_helpers::{
    assert_json_eq, assert_json_has_key, assert_not_empty, assert_starts_with, format_span_id,
    format_trace_id, generate_test_id, get_bool_from_json, get_i64_from_json, get_string_from_json,
    parse_uuid_from_json, wait_for_server,
};
pub use helpers::{ApiClient, OtlpClient, TestDataGenerator, TestFixture};

// Re-export Result and common types for test scenarios
pub use anyhow::Result;
pub use hex;
pub use serde_json::{Value, json};
pub use std::time::Duration;

/// Test configuration loaded from environment variables
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub api_url: String,
    pub grpc_url: String,
    pub admin_email: String,
    pub admin_password: String,
}

impl TestConfig {
    /// Load test configuration from environment variables with defaults
    pub fn from_env() -> Self {
        Self {
            api_url: std::env::var("TEST_API_URL")
                .unwrap_or_else(|_| "http://localhost:9015".to_string()),
            grpc_url: std::env::var("TEST_GRPC_URL")
                .unwrap_or_else(|_| "http://localhost:9016".to_string()),
            admin_email: std::env::var("TEST_ADMIN_EMAIL")
                .unwrap_or_else(|_| "admin@example.com".to_string()),
            admin_password: std::env::var("TEST_ADMIN_PASSWORD")
                .unwrap_or_else(|_| "changeme123".to_string()),
        }
    }
}

/// Test context shared across tests
pub struct TestContext {
    pub config: TestConfig,
    pub api_client: ApiClient,
    pub otlp_client: OtlpClient,
}

impl TestContext {
    /// Create a new test context
    pub fn new() -> Self {
        let config = TestConfig::from_env();
        let api_client = ApiClient::new(config.api_url.clone());
        let otlp_client = OtlpClient::new(config.grpc_url.clone());

        Self {
            config,
            api_client,
            otlp_client,
        }
    }

    /// Login as admin and return authenticated client
    /// If login fails, attempts to register the admin user first
    pub async fn login_as_admin(&self) -> Result<ApiClient> {
        let mut client = ApiClient::new(self.config.api_url.clone());

        // Try to login first
        match client
            .login(&self.config.admin_email, &self.config.admin_password)
            .await
        {
            Ok(_) => Ok(client),
            Err(_) => {
                // Login failed, try to register the admin user
                println!("Admin user not found, registering...");
                let _ = client
                    .register(
                        &self.config.admin_email,
                        &self.config.admin_password,
                        "Test Admin",
                    )
                    .await?;

                // Now try to login again
                client
                    .login(&self.config.admin_email, &self.config.admin_password)
                    .await?;
                Ok(client)
            }
        }
    }

    /// Wait for server to be ready
    pub async fn wait_for_ready(&self, timeout_secs: u64) -> Result<()> {
        wait_for_server(&self.config.api_url, timeout_secs).await
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}
