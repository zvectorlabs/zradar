//! Redis plugin implementation

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use zradar_plugins::{
    Plugin, PluginMetadata, PluginType, ConfigField,
    CachePlugin,
    error::{PluginError, Result},
};

use crate::cache::RedisCache;

/// Redis plugin (cache provider)
pub struct RedisPlugin {
    metadata: PluginMetadata,
    cache: RwLock<Option<Arc<RedisCache>>>,
}

impl RedisPlugin {
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                name: "redis".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "zradar".to_string(),
                description: "Redis cache plugin".to_string(),
                plugin_type: PluginType::Cache,
                dependencies: vec![],
                config_schema: vec![
                    ConfigField {
                        name: "url".to_string(),
                        description: "Redis connection URL".to_string(),
                        required: true,
                        default: None,
                        field_type: "string".to_string(),
                    },
                ],
            },
            cache: RwLock::new(None),
        }
    }
}

impl Default for RedisPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Plugin for RedisPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }
    
    fn validate_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.get("url").and_then(|v| v.as_str()).is_none() {
            return Err(PluginError::InvalidConfig("Redis 'url' is required".to_string()));
        }
        Ok(())
    }
    
    async fn initialize(&self, config: &serde_json::Value) -> Result<()> {
        tracing::info!("Initializing Redis plugin");
        
        let url = config["url"].as_str()
            .ok_or_else(|| PluginError::InvalidConfig("Redis url required".to_string()))?;
        
        let cache = RedisCache::new(url).await
            .map_err(|e| PluginError::InitializationFailed(e.to_string()))?;
        
        *self.cache.write().await = Some(Arc::new(cache));
        
        tracing::info!("Redis plugin initialized");
        Ok(())
    }
    
    async fn health_check(&self) -> Result<bool> {
        if let Some(cache) = self.cache.read().await.as_ref() {
            cache.health_check().await
                .map_err(|e| PluginError::OperationFailed(e.to_string()))
        } else {
            Ok(false)
        }
    }
    
    async fn shutdown(&self) -> Result<()> {
        *self.cache.write().await = None;
        Ok(())
    }
}

#[async_trait]
impl CachePlugin for RedisPlugin {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let cache = self.cache.read().await;
        let cache = cache.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Redis plugin not initialized"))?;
        
        cache.get(key).await
    }
    
    async fn set(&self, key: &str, value: &[u8], ttl_seconds: Option<u64>) -> anyhow::Result<()> {
        let cache = self.cache.read().await;
        let cache = cache.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Redis plugin not initialized"))?;
        
        cache.set(key, value, ttl_seconds).await
    }
    
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let cache = self.cache.read().await;
        let cache = cache.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Redis plugin not initialized"))?;
        
        cache.delete(key).await
    }
    
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let cache = self.cache.read().await;
        let cache = cache.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Redis plugin not initialized"))?;
        
        cache.exists(key).await
    }
}

