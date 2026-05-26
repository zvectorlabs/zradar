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
}

fn default_true() -> bool {
    true
}
