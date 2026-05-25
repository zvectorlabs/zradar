//! Config-based API key authentication (standalone mode) and gateway service token
//! validation (platform mode).

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
                        tenant_id: k.tenant_id.clone(),
                        project_id: k.project_id.clone(),
                        ..Default::default()
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

/// Validates the Agnitiv gateway service token for `auth.mode = "platform"`.
///
/// The gateway presents this shared credential instead of a per-user JWT.
/// On success, returns an empty `RequestContext` — the real tenant, project,
/// and user context is populated from trusted headers in `AuthContext::from_request_parts`.
///
/// Constant-time comparison prevents timing attacks on the token value.
pub struct PlatformAuthenticator {
    /// Expected gateway service token (from `auth.platform.gateway_service_token` config).
    expected_token: String,
}

impl PlatformAuthenticator {
    /// Creates a new platform authenticator with the configured gateway service token.
    pub fn new(gateway_service_token: impl Into<String>) -> Self {
        Self {
            expected_token: gateway_service_token.into(),
        }
    }
}

#[async_trait]
impl Authenticator for PlatformAuthenticator {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
        // Constant-time comparison to prevent timing side-channels
        if !constant_time_eq(token.as_bytes(), self.expected_token.as_bytes()) {
            anyhow::bail!("Invalid gateway service token");
        }
        // Return empty context — headers fill it in the extractor
        Ok(RequestContext::default())
    }
}

/// Constant-time byte slice comparison (no short-circuit on mismatch).
///
/// Not a crypto primitive — avoids trivially visible timing differences.
/// For full protection, use `subtle::ConstantTimeEq` in a future hardening pass.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        // Still run comparison on `a` vs itself to consume similar time
        let _ = a.iter().zip(a.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y));
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
