//! Authentication configuration for OSS standalone zradar.
//!
//! OSS zradar supports one auth mode: static API keys from `[[api_keys]]`.
//! Platform-specific auth (gateway service tokens, remote OTLP key validation)
//! lives exclusively in an external platform wrapper binary.

use crate::WorkspaceId;
use serde::Deserialize;

/// A single API key entry in `config.toml`.
///
/// Example:
/// ```toml
/// [[api_keys]]
/// key          = "zk_live_abc123"
/// workspace_id = "550e8400-e29b-41d4-a716-446655440000"
/// name         = "production ingestor"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyConfig {
    pub key: String,
    pub workspace_id: WorkspaceId,
    #[serde(default)]
    pub name: String,
}

/// Authentication configuration block.
///
/// OSS standalone zradar uses config API keys only. An external platform wrapper
/// can add its own auth strategies without modifying this struct.
///
/// `Default` is implemented by hand so that it matches the serde defaults
/// exactly. `Config::load()` uses `AuthConfig::default()` when no config file
/// is present, so a derived `Default` (all `false`) would silently disable
/// OTLP API-key enforcement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AuthConfig {
    /// When `false`, OTLP gRPC accepts requests without an API key. Default: `true`.
    #[serde(default = "default_true")]
    pub otlp_require_api_key: bool,
    /// Test-only context simulation for functional/E2E tests.
    ///
    /// When `true`, standalone API-key auth accepts `x-workspace-id` header
    /// after validating the bearer token. This lets tests
    /// simulate many API keys/contexts with one static key. Keep this `false`
    /// in all non-test configs.
    #[serde(default)]
    pub allow_test_header_context: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            otlp_require_api_key: default_true(),
            allow_test_header_context: false,
        }
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_serde_defaults() {
        let from_toml: AuthConfig = toml::from_str("").expect("empty auth config parses");
        let from_default = AuthConfig::default();
        assert_eq!(from_default, from_toml);
        assert!(from_default.otlp_require_api_key);
        assert!(!from_default.allow_test_header_context);
    }
}
