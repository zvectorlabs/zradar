//! Local storage plugin implementation

use async_trait::async_trait;
use std::sync::Arc;

use zradar_plugins::{
    Plugin, PluginMetadata, PluginType, ConfigField,
    StoragePlugin,
    error::Result,
};
use zradar_traits::BlockStorage;

use crate::storage::LocalBlockStorage;

/// Local filesystem storage plugin
pub struct LocalStoragePlugin {
    metadata: PluginMetadata,
}

impl LocalStoragePlugin {
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                name: "local".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "zradar".to_string(),
                description: "Local filesystem storage for development".to_string(),
                plugin_type: PluginType::Storage,
                dependencies: vec![],
                config_schema: vec![
                    ConfigField {
                        name: "path".to_string(),
                        field_type: "string".to_string(),
                        required: false,
                        default: Some(serde_json::json!("./data/trace-batches")),
                        description: "Base path for local storage".to_string(),
                    },
                ],
            },
        }
    }
}

impl Default for LocalStoragePlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Plugin for LocalStoragePlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }
    
    async fn initialize(&self, _config: &serde_json::Value) -> Result<()> {
        tracing::info!("Local storage plugin initialized");
        Ok(())
    }
    
    async fn shutdown(&self) -> Result<()> {
        tracing::info!("Local storage plugin shutdown");
        Ok(())
    }
    
    fn validate_config(&self, config: &serde_json::Value) -> Result<()> {
        // Path is optional, so no validation needed
        if let Some(path) = config.get("path") {
            if !path.is_string() {
                return Err(zradar_plugins::error::PluginError::InvalidConfig(
                    "path must be a string".to_string()
                ).into());
            }
        }
        Ok(())
    }
    
    async fn health_check(&self) -> Result<bool> {
        // Local storage is always healthy if the plugin is loaded
        Ok(true)
    }
}

#[async_trait]
impl StoragePlugin for LocalStoragePlugin {
    async fn create_storage(&self, config: &serde_json::Value) -> Result<Arc<dyn BlockStorage>> {
        let path = config.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("./data/trace-batches");
        
        tracing::info!(path = path, "Creating local block storage");
        
        Ok(Arc::new(LocalBlockStorage::new(path)))
    }
}
