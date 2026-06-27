//! Authentication configuration for OSS standalone zradar.
//!
//! OSS zradar supports one auth mode: static API keys from `[[api_keys]]`.
//! Platform-specific auth (gateway service tokens, remote OTLP key validation)
//! lives exclusively in an external platform wrapper binary.

use serde::Deserialize;

/// A single API key entry in `config.toml`.
///
/// Example:
/// ```toml
/// [[api_keys]]
/// key        = "zk_live_abc123"
/// tenant_id  = "acme"
/// project_id = "prod"
/// name       = "production ingestor"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyConfig {
    pub key: String,
    pub tenant_id: String,
    pub project_id: String,
    #[serde(default)]
    pub name: String,
}

/// Authentication configuration block.
///
/// OSS standalone zradar uses config API keys only. An external platform wrapper
/// can add its own auth strategies without modifying this struct.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// When `false`, OTLP gRPC accepts requests without an API key. Default: `true`.
    #[serde(default = "default_true")]
    pub otlp_require_api_key: bool,
    /// Test-only context simulation for functional/E2E tests.
    ///
    /// When `true`, standalone API-key auth accepts `x-tenant-id` and
    /// `x-project-id` headers after validating the bearer token. This lets tests
    /// simulate many API keys/contexts with one static key. Keep this `false`
    /// in all non-test configs.
    #[serde(default)]
    pub allow_test_header_context: bool,
}

fn default_true() -> bool {
    true
}
