//! Plugin configuration types

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::plugin::MigrationOptions;

/// Plugin system configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginConfig {
    /// Directory to scan for plugin .so/.dylib files
    #[serde(default = "default_plugin_dir")]
    pub plugin_dir: String,
    
    /// Plugins to load (in order)
    #[serde(default)]
    pub enabled: Vec<String>,
    
    /// Backend assignments
    #[serde(default)]
    pub backends: BackendConfig,
    
    /// Global migration settings
    #[serde(default)]
    pub migrations: MigrationConfig,
    
    /// Plugin-specific configurations
    #[serde(default, flatten)]
    pub configs: HashMap<String, serde_json::Value>,
}

/// Global migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Run migrations automatically on plugin initialization
    #[serde(default)]
    pub auto_migrate: bool,
    
    /// Fail startup if checksums don't match
    #[serde(default = "default_true")]
    pub strict_checksums: bool,
    
    /// Per-plugin migration directories
    #[serde(default)]
    pub plugin_migrations: HashMap<String, PluginMigrationConfig>,
}

/// Per-plugin migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMigrationConfig {
    /// Path to migrations directory
    pub migrations_dir: String,
    /// Override auto_migrate for this plugin
    pub auto_migrate: Option<bool>,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            auto_migrate: false,
            strict_checksums: true,
            plugin_migrations: HashMap::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_plugin_dir() -> String {
    "./plugins".to_string()
}

/// Backend selection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    /// Storage backend (default: "postgres")
    #[serde(default = "default_postgres")]
    pub storage: String,
    
    /// Job queue backend (default: "postgres")
    #[serde(default = "default_postgres")]
    pub queue: String,
    
    /// Telemetry writer backend (default: "postgres")
    #[serde(default = "default_postgres")]
    pub telemetry_writer: String,
    
    /// Telemetry reader backend (default: "postgres")
    #[serde(default = "default_postgres")]
    pub telemetry_reader: String,
    
    /// Cache backend (default: "memory")
    #[serde(default = "default_memory")]
    pub cache: String,
    
    /// Auth backend (default: "postgres")
    #[serde(default = "default_postgres")]
    pub auth: String,
}

fn default_postgres() -> String {
    "postgres".to_string()
}

fn default_memory() -> String {
    "memory".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            storage: default_postgres(),
            queue: default_postgres(),
            telemetry_writer: default_postgres(),
            telemetry_reader: default_postgres(),
            cache: default_memory(),
            auth: default_postgres(),
        }
    }
}

impl PluginConfig {
    /// Load from TOML file
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// Get configuration for a specific plugin
    pub fn get_plugin_config(&self, name: &str) -> serde_json::Value {
        self.configs.get(name)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    }
    
    /// Get migration options for a specific plugin
    pub fn get_migration_options(&self, plugin_name: &str) -> MigrationOptions {
        let plugin_config = self.migrations.plugin_migrations.get(plugin_name);
        
        MigrationOptions {
            migrations_dir: plugin_config
                .map(|c| c.migrations_dir.clone())
                .unwrap_or_else(|| format!("./migrations_{}", plugin_name)),
            auto_migrate: plugin_config
                .and_then(|c| c.auto_migrate)
                .unwrap_or(self.migrations.auto_migrate),
            strict_checksums: self.migrations.strict_checksums,
            dry_run: false,
        }
    }
}

