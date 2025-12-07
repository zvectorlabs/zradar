//! S3 plugin implementation

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use zradar_plugins::{
    ConfigField, Plugin, PluginMetadata, PluginType, StoragePlugin,
    error::{PluginError, Result},
};
use zradar_traits::BlockStorage;

use crate::storage::S3BlockStorage;

/// S3 storage plugin
pub struct S3Plugin {
    metadata: PluginMetadata,
    storage: RwLock<Option<Arc<S3BlockStorage>>>,
}

impl S3Plugin {
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                name: "s3".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "zradar".to_string(),
                description: "AWS S3 block storage plugin".to_string(),
                plugin_type: PluginType::Storage,
                dependencies: vec![],
                config_schema: vec![
                    ConfigField {
                        name: "bucket".to_string(),
                        description: "S3 bucket name".to_string(),
                        required: true,
                        default: None,
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "region".to_string(),
                        description: "AWS region".to_string(),
                        required: false,
                        default: Some(serde_json::json!("us-east-1")),
                        field_type: "string".to_string(),
                    },
                ],
            },
            storage: RwLock::new(None),
        }
    }
}

impl Default for S3Plugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Plugin for S3Plugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.get("bucket").and_then(|v| v.as_str()).is_none() {
            return Err(PluginError::InvalidConfig(
                "S3 'bucket' is required".to_string(),
            ));
        }
        Ok(())
    }

    async fn initialize(&self, config: &serde_json::Value) -> Result<()> {
        tracing::info!("Initializing S3 plugin");

        let storage = S3BlockStorage::from_config(config)
            .await
            .map_err(|e| PluginError::InitializationFailed(e.to_string()))?;

        *self.storage.write().await = Some(Arc::new(storage));

        tracing::info!("S3 plugin initialized");
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        Ok(self.storage.read().await.is_some())
    }

    async fn shutdown(&self) -> Result<()> {
        *self.storage.write().await = None;
        Ok(())
    }
}

#[async_trait]
impl StoragePlugin for S3Plugin {
    async fn create_storage(&self, config: &serde_json::Value) -> Result<Arc<dyn BlockStorage>> {
        let storage = S3BlockStorage::from_config(config)
            .await
            .map_err(|e| PluginError::OperationFailed(e.to_string()))?;

        Ok(Arc::new(storage))
    }
}
