//! API Key repository trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// API Key for programmatic access
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct ApiKey {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    #[serde(skip_serializing)]
    pub key_hash: String,
    pub key_prefix: String,
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<String>,
    pub ip_whitelist: Option<Vec<String>>,
    pub rate_limit_per_minute: Option<i32>,
    pub is_active: bool,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

/// Request to create an API key
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub description: Option<String>,
    pub permissions: Option<Vec<String>>,
    pub expires_in_days: Option<u32>,
    pub rate_limit_per_minute: Option<i32>,
}

/// Response after creating an API key (includes the actual key)
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CreateApiKeyResponse {
    pub id: Uuid,
    /// The actual API key - ONLY shown once!
    pub key: String,
    pub key_prefix: String,
    pub name: String,
    pub permissions: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// API Key response (without sensitive data)
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct ApiKeyResponse {
    pub id: Uuid,
    pub key_prefix: String,
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl From<ApiKey> for ApiKeyResponse {
    fn from(key: ApiKey) -> Self {
        Self {
            id: key.id,
            key_prefix: key.key_prefix,
            name: key.name,
            description: key.description,
            permissions: key.permissions,
            is_active: key.is_active,
            created_at: key.created_at,
            last_used_at: key.last_used_at,
            expires_at: key.expires_at,
        }
    }
}

/// Repository trait for API key persistence
#[async_trait]
pub trait ApiKeyRepository: Send + Sync {
    /// Create a new API key
    async fn create_key(
        &self,
        org_id: Uuid,
        project_id: Uuid,
        key_hash: String,
        key_prefix: String,
        req: CreateApiKeyRequest,
        created_by: Uuid,
    ) -> anyhow::Result<ApiKey>;

    /// Get API key by ID
    async fn get_key(&self, id: Uuid) -> anyhow::Result<Option<ApiKey>>;

    /// Get API key by hash
    async fn get_key_by_hash(&self, hash: &str) -> anyhow::Result<Option<ApiKey>>;

    /// List API keys for a project
    async fn list_keys(&self, org_id: Uuid, project_id: Uuid) -> anyhow::Result<Vec<ApiKey>>;

    /// Revoke (deactivate) an API key
    async fn revoke_key(&self, id: Uuid) -> anyhow::Result<()>;

    /// Delete an API key
    async fn delete_key(&self, id: Uuid) -> anyhow::Result<()>;

    /// Update last used timestamp
    async fn update_last_used(&self, id: Uuid) -> anyhow::Result<()>;
}
