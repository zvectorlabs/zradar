//! Config-based API key authentication.

use std::collections::HashMap;

use async_trait::async_trait;
use zradar_models::{ApiKeyConfig, RequestContext};
use zradar_traits::Authenticator;

/// Validates bearer tokens against a static map loaded from `config.toml`.
///
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
