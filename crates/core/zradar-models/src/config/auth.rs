//! Authentication configuration

use serde::Deserialize;

/// A single API key entry in the config file.
///
/// Example in `config.toml`:
/// ```toml
/// [[api_keys]]
/// key = "zk_live_abc123"
/// tenant_id = "acme"
/// project_id = "prod"
/// name = "production ingestor"
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
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// When false, OTLP gRPC accepts requests without an API key. Default: true.
    #[serde(default = "default_true")]
    pub otlp_require_api_key: bool,
}

fn default_true() -> bool {
    true
}
