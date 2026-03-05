//! Authentication and authorization configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AdminApiConfig {
    pub jwt_secret: Option<String>,

    #[serde(default = "default_jwt_expiry")]
    pub jwt_expiry_hours: Option<u32>,

    #[serde(default = "default_admin_port")]
    pub admin_api_port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub type_: String, // "api-key", "jwt", "mtls"

    #[serde(default)]
    pub api_keys: Vec<ApiKeyConfig>,

    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_seconds: Option<u64>,

    /// When false, OTLP gRPC (protobuf) accepts requests without API key. Default: true.
    #[serde(default = "default_otlp_require_api_key")]
    pub otlp_require_api_key: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyConfig {
    pub key: String,
    pub tenant_id: String,
    pub project_id: String,
    pub name: String,

    #[serde(default)]
    pub permissions: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            type_: "api-key".to_string(),
            api_keys: Vec::new(),
            cache_ttl_seconds: Some(300),
            otlp_require_api_key: Some(true),
        }
    }
}

// Default functions
fn default_admin_port() -> Option<u16> {
    Some(8080)
}
fn default_otlp_require_api_key() -> Option<bool> {
    Some(true)
}
fn default_jwt_expiry() -> Option<u32> {
    Some(24)
}
fn default_cache_ttl() -> Option<u64> {
    Some(300)
}
