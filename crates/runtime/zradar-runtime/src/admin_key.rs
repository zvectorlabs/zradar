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
    allow_test_header_context: bool,
}

impl ApiKeyAdminAuthorizer {
    /// Build from the `[[api_keys]]` section of the config.
    pub fn from_config(api_keys: &[ApiKeyConfig]) -> Self {
        Self::from_config_with_test_header_context(api_keys, false)
    }

    /// Build from config and optionally enable test-only header context.
    pub fn from_config_with_test_header_context(
        api_keys: &[ApiKeyConfig],
        allow_test_header_context: bool,
    ) -> Self {
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
        Self {
            keys,
            allow_test_header_context,
        }
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

        if self.allow_test_header_context {
            if let Some(val) = header_str(headers, "x-tenant-id") {
                context.tenant_id = val.to_string();
            }
            if let Some(val) = header_str(headers, "x-project-id") {
                context.project_id = val.to_string();
            }
        }

        Ok(AdminAuth {
            context,
            capability_keys: Vec::new(),
        })
    }
}
