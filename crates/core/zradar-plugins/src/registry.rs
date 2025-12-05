//! Plugin registry - manages plugin lifecycle and discovery

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{info, warn, error};

use crate::error::{PluginError, Result};
use crate::plugin::*;

/// Plugin registry - singleton that manages all loaded plugins
pub struct PluginRegistry {
    /// Storage plugins by name
    storage_plugins: RwLock<HashMap<String, Arc<dyn StoragePlugin>>>,
    /// Queue plugins by name
    queue_plugins: RwLock<HashMap<String, Arc<dyn QueuePlugin>>>,
    /// Telemetry writer plugins by name
    writer_plugins: RwLock<HashMap<String, Arc<dyn TelemetryWriterPlugin>>>,
    /// Telemetry reader plugins by name
    reader_plugins: RwLock<HashMap<String, Arc<dyn TelemetryReaderPlugin>>>,
    /// Cache plugins by name
    cache_plugins: RwLock<HashMap<String, Arc<dyn CachePlugin>>>,
    /// Score storage plugins by name
    score_plugins: RwLock<HashMap<String, Arc<dyn ScoreStoragePlugin>>>,
    /// Migratable plugins by name (plugins that support migrations)
    migratable_plugins: RwLock<HashMap<String, Arc<dyn MigratablePlugin>>>,
    /// All plugins (for lifecycle management)
    all_plugins: RwLock<HashMap<String, Arc<dyn Plugin>>>,
}

impl PluginRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            storage_plugins: RwLock::new(HashMap::new()),
            queue_plugins: RwLock::new(HashMap::new()),
            writer_plugins: RwLock::new(HashMap::new()),
            reader_plugins: RwLock::new(HashMap::new()),
            cache_plugins: RwLock::new(HashMap::new()),
            score_plugins: RwLock::new(HashMap::new()),
            migratable_plugins: RwLock::new(HashMap::new()),
            all_plugins: RwLock::new(HashMap::new()),
        }
    }
    
    // =========================================================================
    // Registration methods
    // =========================================================================
    
    /// Register a storage plugin
    pub fn register_storage(&self, plugin: Arc<dyn StoragePlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.storage_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        // Also register in all_plugins for lifecycle management
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered storage plugin");
        Ok(())
    }
    
    /// Register a queue plugin
    pub fn register_queue(&self, plugin: Arc<dyn QueuePlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.queue_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered queue plugin");
        Ok(())
    }
    
    /// Register a telemetry writer plugin
    pub fn register_writer(&self, plugin: Arc<dyn TelemetryWriterPlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.writer_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered telemetry writer plugin");
        Ok(())
    }
    
    /// Register a telemetry reader plugin
    pub fn register_reader(&self, plugin: Arc<dyn TelemetryReaderPlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.reader_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered telemetry reader plugin");
        Ok(())
    }
    
    /// Register a cache plugin
    pub fn register_cache(&self, plugin: Arc<dyn CachePlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.cache_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered cache plugin");
        Ok(())
    }
    
    /// Register a score storage plugin
    pub fn register_score_storage(&self, plugin: Arc<dyn ScoreStoragePlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.score_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        // Also register as migratable
        {
            let mut migratable = self.migratable_plugins.write().unwrap();
            migratable.insert(name.clone(), plugin.clone() as Arc<dyn MigratablePlugin>);
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered score storage plugin");
        Ok(())
    }
    
    /// Register a migratable plugin (for plugins that don't fit other categories)
    pub fn register_migratable(&self, plugin: Arc<dyn MigratablePlugin>) -> Result<()> {
        let name = plugin.metadata().name.clone();
        
        {
            let mut plugins = self.migratable_plugins.write().unwrap();
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyRegistered(name));
            }
            plugins.insert(name.clone(), plugin.clone());
        }
        
        {
            let mut all = self.all_plugins.write().unwrap();
            all.insert(name.clone(), plugin as Arc<dyn Plugin>);
        }
        
        info!(plugin = %name, "Registered migratable plugin");
        Ok(())
    }
    
    // =========================================================================
    // Retrieval methods
    // =========================================================================
    
    /// Get a storage plugin by name
    pub fn get_storage(&self, name: &str) -> Option<Arc<dyn StoragePlugin>> {
        self.storage_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a queue plugin by name
    pub fn get_queue(&self, name: &str) -> Option<Arc<dyn QueuePlugin>> {
        self.queue_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a telemetry writer plugin by name
    pub fn get_writer(&self, name: &str) -> Option<Arc<dyn TelemetryWriterPlugin>> {
        self.writer_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a telemetry reader plugin by name
    pub fn get_reader(&self, name: &str) -> Option<Arc<dyn TelemetryReaderPlugin>> {
        self.reader_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a cache plugin by name
    pub fn get_cache(&self, name: &str) -> Option<Arc<dyn CachePlugin>> {
        self.cache_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a score storage plugin by name
    pub fn get_score_storage(&self, name: &str) -> Option<Arc<dyn ScoreStoragePlugin>> {
        self.score_plugins.read().unwrap().get(name).cloned()
    }
    
    /// Get a migratable plugin by name
    pub fn get_migratable(&self, name: &str) -> Option<Arc<dyn MigratablePlugin>> {
        self.migratable_plugins.read().unwrap().get(name).cloned()
    }
    
    // =========================================================================
    // Discovery methods
    // =========================================================================
    
    /// List all storage plugins
    pub fn list_storage_plugins(&self) -> Vec<PluginMetadata> {
        self.storage_plugins.read().unwrap()
            .values()
            .map(|p| p.metadata().clone())
            .collect()
    }
    
    /// List all queue plugins
    pub fn list_queue_plugins(&self) -> Vec<PluginMetadata> {
        self.queue_plugins.read().unwrap()
            .values()
            .map(|p| p.metadata().clone())
            .collect()
    }
    
    /// List all telemetry writer plugins
    pub fn list_writer_plugins(&self) -> Vec<PluginMetadata> {
        self.writer_plugins.read().unwrap()
            .values()
            .map(|p| p.metadata().clone())
            .collect()
    }
    
    /// List all plugins
    pub fn list_all_plugins(&self) -> Vec<PluginMetadata> {
        self.all_plugins.read().unwrap()
            .values()
            .map(|p| p.metadata().clone())
            .collect()
    }
    
    // =========================================================================
    // Lifecycle methods
    // =========================================================================
    
    /// Initialize all registered plugins
    pub async fn initialize_all(&self, configs: &HashMap<String, serde_json::Value>) -> Result<()> {
        let plugins: Vec<_> = self.all_plugins.read().unwrap()
            .iter()
            .map(|(name, plugin)| (name.clone(), plugin.clone()))
            .collect();
        
        for (name, plugin) in plugins {
            let config = configs.get(&name)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            
            info!(plugin = %name, "Initializing plugin");
            
            if let Err(e) = plugin.validate_config(&config) {
                error!(plugin = %name, error = %e, "Plugin config validation failed");
                return Err(e);
            }
            
            if let Err(e) = plugin.initialize(&config).await {
                error!(plugin = %name, error = %e, "Plugin initialization failed");
                return Err(e);
            }
            
            info!(plugin = %name, "Plugin initialized successfully");
        }
        
        Ok(())
    }
    
    /// Shutdown all plugins gracefully
    pub async fn shutdown_all(&self) -> Result<()> {
        let plugins: Vec<_> = self.all_plugins.read().unwrap()
            .iter()
            .map(|(name, plugin)| (name.clone(), plugin.clone()))
            .collect();
        
        for (name, plugin) in plugins {
            info!(plugin = %name, "Shutting down plugin");
            
            if let Err(e) = plugin.shutdown().await {
                warn!(plugin = %name, error = %e, "Plugin shutdown error");
            }
        }
        
        Ok(())
    }
    
    /// Health check all plugins
    pub async fn health_check_all(&self) -> HashMap<String, bool> {
        let plugins: Vec<_> = self.all_plugins.read().unwrap()
            .iter()
            .map(|(name, plugin)| (name.clone(), plugin.clone()))
            .collect();
        
        let mut results = HashMap::new();
        
        for (name, plugin) in plugins {
            let healthy = plugin.health_check().await.unwrap_or(false);
            results.insert(name, healthy);
        }
        
        results
    }
    
    // =========================================================================
    // Migration methods
    // =========================================================================
    
    /// Run migrations for a specific plugin
    pub async fn run_plugin_migrations(
        &self,
        plugin_name: &str,
        options: &MigrationOptions,
    ) -> Result<()> {
        let plugin = self.migratable_plugins.read().unwrap()
            .get(plugin_name)
            .cloned();
        
        match plugin {
            Some(p) => {
                info!(plugin = %plugin_name, "Running migrations");
                
                // Check status first
                let status = p.migration_status(options).await
                    .map_err(|e| PluginError::MigrationFailed(e.to_string()))?;
                
                match status {
                    MigrationStatus::UpToDate => {
                        info!(plugin = %plugin_name, "Migrations up to date");
                        Ok(())
                    }
                    MigrationStatus::Pending { count, names } => {
                        info!(
                            plugin = %plugin_name,
                            count = count,
                            pending = ?names,
                            "Applying pending migrations"
                        );
                        
                        let applied = p.run_migrations(options).await
                            .map_err(|e| PluginError::MigrationFailed(e.to_string()))?;
                        
                        for m in &applied {
                            info!(
                                plugin = %plugin_name,
                                migration = %m.name,
                                duration_ms = m.duration_ms,
                                "Applied migration"
                            );
                        }
                        
                        Ok(())
                    }
                    MigrationStatus::ChecksumMismatch { migration } => {
                        Err(PluginError::MigrationChecksumMismatch(format!(
                            "Plugin {} migration {} checksum mismatch",
                            plugin_name, migration
                        )))
                    }
                    MigrationStatus::Error(e) => {
                        Err(PluginError::MigrationFailed(e))
                    }
                }
            }
            None => {
                // Plugin doesn't support migrations, that's OK
                info!(plugin = %plugin_name, "Plugin does not support migrations");
                Ok(())
            }
        }
    }
    
    /// Get migration status for all migratable plugins
    pub async fn migration_status_all(
        &self,
        options_map: &HashMap<String, MigrationOptions>,
    ) -> HashMap<String, MigrationStatus> {
        let plugins: Vec<_> = self.migratable_plugins.read().unwrap()
            .iter()
            .map(|(name, plugin)| (name.clone(), plugin.clone()))
            .collect();
        
        let mut results = HashMap::new();
        
        for (name, plugin) in plugins {
            let opts = options_map.get(&name).cloned().unwrap_or_default();
            let status = plugin.migration_status(&opts).await
                .unwrap_or_else(|e| MigrationStatus::Error(e.to_string()));
            results.insert(name, status);
        }
        
        results
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

