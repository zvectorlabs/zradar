//! Config-key–based Admin HTTP authorizer for OSS standalone mode.

use async_trait::async_trait;
use axum::http::{HeaderMap, header::AUTHORIZATION};
use std::collections::HashMap;
use zradar_models::{ApiKeyConfig, RequestContext};
use zradar_traits::{AdminAuth, AdminAuthorizer};

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Validates Admin HTTP requests against static API keys from `config.toml`.
///
/// Capabilities list is always empty in standalone mode — handlers pass
/// without capability checks by checking `AuthMode::Standalone`.
pub struct ApiKeyAdminAuthorizer {
    keys: HashMap<String, RequestContext>,
}

impl ApiKeyAdminAuthorizer {
    /// Build from the `[[api_keys]]` section of the config.
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
impl AdminAuthorizer for ApiKeyAdminAuthorizer {
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AdminAuth> {
        let bearer = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| anyhow::anyhow!("Missing Authorization header"))?;

        let mut context = self
            .keys
            .get(bearer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Invalid API key"))?;

        // Honour x-tenant-id / x-project-id override headers (intra-org routing).
        // This preserves the behaviour of the old build_standalone_context function.
        if let Some(val) = header_str(headers, "x-tenant-id") {
            context.tenant_id = val.to_string();
        }
        if let Some(val) = header_str(headers, "x-project-id") {
            context.project_id = val.to_string();
        }

        Ok(AdminAuth {
            context,
            capability_keys: Vec::new(),
        })
    }
}
