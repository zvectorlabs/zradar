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

/// Authentication mode for the zradar server.
///
/// - `Standalone`: Classic deployment — callers authenticate with a static API key.
///   Tenant and project context are bound to the key itself; optional header overrides
///   are allowed for intra-org routing.
///
/// - `Platform`: Gateway-managed deployment. The gateway presents a shared
///   `gateway_service_token` and forwards trusted request context headers;
///   ad-hoc API key authentication is disabled in this mode.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Static API key authentication (default, backward-compatible).
    #[default]
    Standalone,
    /// Gateway service token + trusted context headers.
    Platform,
}

/// Configuration for platform mode authentication.
///
/// Only used when `auth.mode = "platform"`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PlatformAuthConfig {
    /// Shared service credential expected in the `Authorization: Bearer <token>` header.
    ///
    /// This is the `ZRADAR_GATEWAY_SERVICE_TOKEN` known to the Agnitiv gateway.
    /// Requests that do not present this exact token are rejected with 401.
    ///
    /// In production this should be a long random secret (≥32 bytes of entropy).
    /// For local dev, any non-empty string works.
    pub gateway_service_token: String,
}

/// Authentication configuration block.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// Authentication mode. Default: `standalone`.
    ///
    /// Set `mode = "platform"` to accept Agnitiv gateway credentials.
    #[serde(default)]
    pub mode: AuthMode,

    /// Platform mode settings (required when `mode = "platform"`).
    #[serde(default)]
    pub platform: PlatformAuthConfig,

    /// When false, OTLP gRPC accepts requests without an API key. Default: true.
    #[serde(default = "default_true")]
    pub otlp_require_api_key: bool,
}

impl AuthConfig {
    /// Returns true if running in platform (Agnitiv gateway) mode.
    pub fn is_platform_mode(&self) -> bool {
        self.mode == AuthMode::Platform
    }
}

fn default_true() -> bool {
    true
}
