//! Database-backed API key authentication with caching

use async_trait::async_trait;
use chrono::Utc;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::api_keys::service::ApiKeyValidator;
use crate::auth::api_key_validator::ApiKeyValidator as AuthApiKeyValidator;
use crate::errors::{ControlError, Result};
use crate::audit::{AuditEvent, AuditStatus, AuditLogger};
use crate::domain::api_keys::ApiKeyRepository;

/// Cached API key information
#[derive(Clone)]
pub struct CachedKeyInfo {
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub key_id: Uuid,
    pub permissions: Vec<String>,
    pub is_active: bool,
    pub expires_at: Option<chrono::DateTime<Utc>>,
    cached_at: std::time::Instant,
}

/// Request context after successful authentication
#[derive(Clone, Debug)]
pub struct RequestContext {
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub key_id: Uuid,
    pub permissions: Vec<String>,
}

/// API Key authentication service
pub struct ApiKeyAuth {
    storage: Arc<dyn ApiKeyRepository>,
    audit: Arc<dyn AuditLogger>,
    cache: Arc<RwLock<LruCache<String, CachedKeyInfo>>>,
    cache_ttl: std::time::Duration,
}

impl ApiKeyAuth {
    pub fn new(
        storage: Arc<dyn ApiKeyRepository>,
        audit: Arc<dyn AuditLogger>,
        cache_size: usize,
        cache_ttl_secs: u64,
    ) -> Self {
        let cache_size = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap());
        
        Self {
            storage,
            audit,
            cache: Arc::new(RwLock::new(LruCache::new(cache_size))),
            cache_ttl: std::time::Duration::from_secs(cache_ttl_secs),
        }
    }

    /// Validate an API key and return request context
    pub async fn validate(&self, key: &str) -> Result<RequestContext> {
        // Hash the key for lookup
        let key_hash = Self::hash_key(key);

        // Check cache first
        if let Some(cached) = self.get_from_cache(&key_hash).await
            && self.is_cache_valid(&cached) {
                tracing::debug!(key_id = %cached.key_id, "API key found in cache");
                
                // Async update last_used_at (fire and forget)
                let storage = self.storage.clone();
                let key_id = cached.key_id;
                tokio::spawn(async move {
                    let _ = storage.update_last_used(key_id).await;
                });

                return Ok(RequestContext {
                    organization_id: cached.organization_id,
                    project_id: cached.project_id,
                    key_id: cached.key_id,
                    permissions: cached.permissions,
                });
            }

        // Not in cache or expired - fetch from database
        let api_key = self.storage.get_key_by_hash(&key_hash).await?
            .ok_or_else(|| {
                tracing::warn!("API key validation failed: key not found");
                ControlError::Unauthorized("Invalid API key".to_string())
            })?;

        // Check if key is active
        if !api_key.is_active {
            self.log_auth_failure(None, None, "API key is inactive").await;
            return Err(ControlError::Unauthorized("API key is inactive".to_string()));
        }

        // Check if key has expired
        if let Some(expires_at) = api_key.expires_at
            && expires_at < Utc::now() {
                self.log_auth_failure(
                    Some(api_key.organization_id),
                    Some(api_key.id),
                    "API key has expired"
                ).await;
                return Err(ControlError::Unauthorized("API key has expired".to_string()));
            }

        // Cache the key info
        let cached_info = CachedKeyInfo {
            organization_id: api_key.organization_id,
            project_id: api_key.project_id,
            key_id: api_key.id,
            permissions: api_key.permissions.clone(),
            is_active: api_key.is_active,
            expires_at: api_key.expires_at,
            cached_at: std::time::Instant::now(),
        };
        self.put_in_cache(key_hash.clone(), cached_info).await;

        // Update last_used_at asynchronously
        let storage = self.storage.clone();
        let key_id = api_key.id;
        tokio::spawn(async move {
            let _ = storage.update_last_used(key_id).await;
        });

        // Log successful authentication
        tracing::info!(
            key_id = %api_key.id,
            org_id = %api_key.organization_id,
            project_id = %api_key.project_id,
            "API key authenticated successfully"
        );

        Ok(RequestContext {
            organization_id: api_key.organization_id,
            project_id: api_key.project_id,
            key_id: api_key.id,
            permissions: api_key.permissions,
        })
    }

    /// Hash an API key using bcrypt (for storage)
    pub fn hash_key(key: &str) -> String {
        // For lookups, use a fast hash (not bcrypt)
        // In production, you'd want to use bcrypt::hash for storage
        // but for lookups we use SHA256
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Hash a key for storage (using bcrypt)
    pub fn hash_key_for_storage(key: &str) -> Result<String> {
        bcrypt::hash(key, bcrypt::DEFAULT_COST)
            .map_err(|_| ControlError::PasswordHash)
    }

    /// Verify a key against a bcrypt hash
    pub fn verify_key(key: &str, hash: &str) -> Result<bool> {
        bcrypt::verify(key, hash)
            .map_err(|_| ControlError::PasswordHash)
    }

    /// Generate a new API key
    pub fn generate_key(prefix: &str) -> String {
        use rand::Rng;
        let random_part: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        
        format!("{}_{}", prefix, random_part)
    }

    // Cache operations
    async fn get_from_cache(&self, key_hash: &str) -> Option<CachedKeyInfo> {
        let mut cache = self.cache.write().await;
        cache.get(key_hash).cloned()
    }

    async fn put_in_cache(&self, key_hash: String, info: CachedKeyInfo) {
        let mut cache = self.cache.write().await;
        cache.put(key_hash, info);
    }

    /// Invalidate cache entry for a specific API key
    /// This should be called when an API key is revoked, deleted, or updated
    pub async fn invalidate_cache_by_key_id(&self, key_id: Uuid) {
        // We need to find and remove the cache entry
        // Since cache is keyed by hash, we need to scan for the matching key_id
        let mut cache = self.cache.write().await;
        let keys_to_remove: Vec<String> = cache
            .iter()
            .filter(|(_, info)| info.key_id == key_id)
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in keys_to_remove {
            cache.pop(&key);
            tracing::debug!(key_id = %key_id, "Invalidated cache entry for API key");
        }
    }

    fn is_cache_valid(&self, cached: &CachedKeyInfo) -> bool {
        // Check if cache entry is still valid
        if !cached.is_active {
            return false;
        }

        // Check TTL
        if cached.cached_at.elapsed() > self.cache_ttl {
            return false;
        }

        // Check expiration
        if let Some(expires_at) = cached.expires_at
            && expires_at < Utc::now() {
                return false;
            }

        true
    }

    /// Revoke an API key from cache (called after database revocation)
    pub async fn revoke(&self, key_id: Uuid) -> Result<()> {
        self.invalidate_cache_by_key_id(key_id).await;
        Ok(())
    }

    async fn log_auth_failure(&self, org_id: Option<Uuid>, key_id: Option<Uuid>, reason: &str) {
        let event = AuditEvent {
            organization_id: org_id,
            user_id: None,
            actor_type: Some("api_key".to_string()),
            actor_id: key_id,
            actor_ip: None,
            action: "api_key.auth_failed".to_string(),
            resource_type: Some("api_key".to_string()),
            resource_id: key_id,
            status: AuditStatus::Failure,
            details: Some(serde_json::json!({ "reason": reason })),
        };

        let _ = self.audit.log(event).await;
    }
}

// Implement the auth layer's ApiKeyValidator trait (has validate + revoke)
#[async_trait]
impl AuthApiKeyValidator for ApiKeyAuth {
    async fn validate(&self, key: &str) -> Result<RequestContext> {
        self.validate(key).await
    }
    
    async fn revoke(&self, key_id: Uuid) -> Result<()> {
        self.invalidate_cache_by_key_id(key_id).await;
        Ok(())
    }
}

// Implement the service layer's ApiKeyValidator trait (has only revoke)
#[async_trait]
impl ApiKeyValidator for ApiKeyAuth {
    async fn revoke(&self, key_id: Uuid) -> Result<()> {
        self.invalidate_cache_by_key_id(key_id).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_key() {
        let key1 = ApiKeyAuth::generate_key("zvr_live");
        let key2 = ApiKeyAuth::generate_key("zvr_live");

        assert!(key1.starts_with("zvr_live_"));
        assert!(key2.starts_with("zvr_live_"));
        assert_ne!(key1, key2);
        assert_eq!(key1.len(), "zvr_live_".len() + 32);
    }

    #[test]
    fn test_hash_key() {
        let key = "zvr_live_abc123def456";
        let hash1 = ApiKeyAuth::hash_key(key);
        let hash2 = ApiKeyAuth::hash_key(key);

        // SHA256 should be deterministic
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA256 produces 64 hex chars
    }
}

