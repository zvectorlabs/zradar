//! Static API-key authenticator for OSS zradar standalone mode.
//!
//! Validates bearer tokens against a map built from `[[api_keys]]` in `config.toml`.
//! This is the only authenticator shipped in the default OSS build.

use std::collections::HashMap;

use async_trait::async_trait;
use zradar_models::{ApiKeyConfig, RequestContext};
use zradar_traits::Authenticator;

/// Validates bearer tokens against a static map loaded from `config.toml`.
///
/// Used in `auth.mode = "standalone"` (the default).
/// O(1) lookup — no database, no cache, no token refresh.
pub struct ConfigAuthenticator {
    keys: HashMap<String, RequestContext>,
}

impl ConfigAuthenticator {
    /// Build from the `[[api_keys]]` section of the config file.
    pub fn from_config(api_keys: &[ApiKeyConfig]) -> Self {
        let keys = api_keys
            .iter()
            .map(|k| {
                (
                    k.key.clone(),
                    RequestContext {
                        workspace_id: k.workspace_id,
                    },
                )
            })
            .collect();
        Self { keys }
    }
}

#[async_trait]
impl Authenticator for ConfigAuthenticator {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
        self.keys
            .get(token)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Invalid API key"))
    }
}

/// Constant-time byte slice comparison (no short-circuit on mismatch).
///
/// Not a crypto primitive — avoids trivially visible timing differences.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        let _ = a
            .iter()
            .zip(a.iter())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y));
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Validates a shared bearer token against a known expected value.
///
/// Intended for use by caller-supplied gateway integrations that need
/// constant-time token comparison without pulling in platform-specific code.
/// Returns `true` when `token` matches `expected`.
pub fn validate_service_token(token: &[u8], expected: &[u8]) -> bool {
    constant_time_eq(token, expected)
}
