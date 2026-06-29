//! Config-key authorizers for OSS standalone mode (shared `[[api_keys]]` store).

use async_trait::async_trait;
use axum::http::{HeaderMap, header::AUTHORIZATION};
use std::collections::HashMap;
use std::sync::Arc;
use zradar_models::{ApiKeyConfig, RequestContext};
use zradar_traits::{AdminAuth, AdminAuthorizer, AuthResolution, QueryAuth, QueryAuthorizer};

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

#[derive(Clone)]
struct ConfigKeyStore {
    keys: Arc<HashMap<String, RequestContext>>,
    allow_test_header_context: bool,
}

impl ConfigKeyStore {
    fn new(api_keys: &[ApiKeyConfig], allow_test_header_context: bool) -> Self {
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
        Self {
            keys: Arc::new(keys),
            allow_test_header_context,
        }
    }

    fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AuthResolution> {
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

        if self.allow_test_header_context
            && let Some(val) = header_str(headers, "x-workspace-id")
            && let Ok(id) = uuid::Uuid::parse_str(val)
        {
            context.workspace_id = id.into();
        }

        Ok(AuthResolution {
            context,
            capability_keys: Vec::new(),
        })
    }
}

/// Validates Query API requests against static API keys from `config.toml`.
pub struct ApiKeyQueryAuthorizer {
    store: ConfigKeyStore,
}

/// Validates Admin API requests against static API keys from `config.toml`.
pub struct ApiKeyAdminAuthorizer {
    store: ConfigKeyStore,
}

impl ApiKeyQueryAuthorizer {
    fn new(store: ConfigKeyStore) -> Self {
        Self { store }
    }
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
        Self {
            store: ConfigKeyStore::new(api_keys, allow_test_header_context),
        }
    }
}

/// Build query and admin authorizers from the same OSS API key table.
pub fn api_key_authorizers_from_config(
    api_keys: &[ApiKeyConfig],
    allow_test_header_context: bool,
) -> (Arc<dyn QueryAuthorizer>, Arc<dyn AdminAuthorizer>) {
    let store = ConfigKeyStore::new(api_keys, allow_test_header_context);
    (
        Arc::new(ApiKeyQueryAuthorizer::new(store.clone())),
        Arc::new(ApiKeyAdminAuthorizer { store }),
    )
}

#[async_trait]
impl QueryAuthorizer for ApiKeyQueryAuthorizer {
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<QueryAuth> {
        self.store.authorize(headers)
    }
}

#[async_trait]
impl AdminAuthorizer for ApiKeyAdminAuthorizer {
    async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AdminAuth> {
        self.store.authorize(headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use uuid::Uuid;

    fn sample_keys() -> Vec<ApiKeyConfig> {
        vec![ApiKeyConfig {
            key: "zk_test".to_string(),
            workspace_id: Uuid::nil().into(),
            name: String::new(),
        }]
    }

    fn auth_headers(token: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        map
    }

    #[tokio::test]
    async fn query_and_admin_share_same_key() {
        let (query, admin) = api_key_authorizers_from_config(&sample_keys(), false);
        let headers = auth_headers("zk_test");

        let q = query.authorize(&headers).await.unwrap();
        let a = admin.authorize(&headers).await.unwrap();
        assert_eq!(q.context.workspace_id, a.context.workspace_id);
    }

    #[tokio::test]
    async fn test_header_context_applies_to_both() {
        let (query, admin) = api_key_authorizers_from_config(&sample_keys(), true);
        let mut headers = auth_headers("zk_test");
        headers.insert(
            "x-workspace-id",
            HeaderValue::from_static("00000000-0000-0000-0000-000000000001"),
        );

        let q = query.authorize(&headers).await.unwrap();
        let a = admin.authorize(&headers).await.unwrap();
        assert_eq!(
            q.context.workspace_id.to_string(),
            "00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(q.context.workspace_id, a.context.workspace_id);
    }
}
