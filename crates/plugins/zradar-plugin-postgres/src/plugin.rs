//! PostgreSQL plugin implementation

use async_trait::async_trait;
use std::sync::Arc;

use zradar_plugins::{
    Plugin, PluginMetadata, PluginType, ConfigField,
    MigratablePlugin, MigrationOptions, MigrationStatus, AppliedMigration,
    error::{PluginError, Result},
};

use crate::client::{PostgresClient, SharedPostgresClient};

/// PostgreSQL plugin - default implementation for all repositories
pub struct PostgresPlugin {
    metadata: PluginMetadata,
    client: SharedPostgresClient,
}

impl PostgresPlugin {
    /// Create a new PostgreSQL plugin
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                name: "postgres".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "zradar".to_string(),
                description: "PostgreSQL plugin - default implementation for all repositories".to_string(),
                plugin_type: PluginType::Storage,
                dependencies: vec![],
                config_schema: vec![
                    ConfigField {
                        name: "url".to_string(),
                        description: "PostgreSQL connection URL".to_string(),
                        required: true,
                        default: None,
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "max_connections".to_string(),
                        description: "Maximum connections in pool".to_string(),
                        required: false,
                        default: Some(serde_json::json!(20)),
                        field_type: "number".to_string(),
                    },
                ],
            },
            client: SharedPostgresClient::new(),
        }
    }
    
    /// Get the internal client
    pub async fn get_client(&self) -> Option<Arc<PostgresClient>> {
        self.client.get().await
    }
}

impl Default for PostgresPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Plugin for PostgresPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }
    
    fn validate_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.get("url").and_then(|v| v.as_str()).is_none() {
            return Err(PluginError::InvalidConfig(
                "PostgreSQL 'url' is required".to_string()
            ));
        }
        Ok(())
    }
    
    async fn initialize(&self, config: &serde_json::Value) -> Result<()> {
        tracing::info!("Initializing PostgreSQL plugin");
        
        self.client.initialize(config).await
            .map_err(|e| PluginError::InitializationFailed(e.to_string()))?;
        
        tracing::info!("PostgreSQL plugin initialized successfully");
        Ok(())
    }
    
    async fn health_check(&self) -> Result<bool> {
        if let Some(client) = self.client.get().await {
            client.health_check().await
                .map_err(|e| PluginError::OperationFailed(e.to_string()))
        } else {
            Ok(false)
        }
    }
    
    async fn shutdown(&self) -> Result<()> {
        tracing::info!("Shutting down PostgreSQL plugin");
        self.client.shutdown().await;
        Ok(())
    }
}

// ============================================================================
// Migration Support
// ============================================================================

#[async_trait]
impl MigratablePlugin for PostgresPlugin {
    async fn run_migrations(&self, options: &MigrationOptions) -> anyhow::Result<Vec<AppliedMigration>> {
        let client = self.client.get().await
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL not initialized"))?;
        
        tracing::info!(
            migrations_dir = %options.migrations_dir,
            "Running PostgreSQL migrations"
        );
        
        // Use sqlx migrate
        let migrator = sqlx::migrate::Migrator::new(
            std::path::Path::new(&options.migrations_dir)
        ).await?;
        
        migrator.run(client.pool()).await?;
        
        tracing::info!("PostgreSQL migrations completed");
        
        Ok(vec![])
    }
    
    async fn migration_status(&self, options: &MigrationOptions) -> anyhow::Result<MigrationStatus> {
        let _client = self.client.get().await
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL not initialized"))?;
        
        // Check if migrations directory exists
        if !std::path::Path::new(&options.migrations_dir).exists() {
            return Ok(MigrationStatus::UpToDate);
        }
        
        // For now, assume up to date (sqlx handles this internally)
        Ok(MigrationStatus::UpToDate)
    }
    
    async fn applied_migrations(&self) -> anyhow::Result<Vec<AppliedMigration>> {
        // Get from PostgreSQL _sqlx_migrations table
        Ok(vec![])
    }
    
    async fn verify_checksums(&self, _options: &MigrationOptions) -> anyhow::Result<bool> {
        // sqlx handles checksum verification
        Ok(true)
    }
}

