//! Plugin loader - handles dynamic loading of plugins

use std::path::Path;
use tracing::{info, warn};

use crate::error::{PluginError, Result};
use crate::registry::PluginRegistry;
use crate::config::PluginConfig;

/// Plugin loader - discovers and loads plugins
pub struct PluginLoader {
    plugin_dir: String,
}

impl PluginLoader {
    /// Create a new plugin loader
    pub fn new(plugin_dir: impl Into<String>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
        }
    }
    
    /// Load plugins from configuration
    pub async fn load_from_config(
        &self,
        config: &PluginConfig,
        registry: &PluginRegistry,
    ) -> Result<()> {
        info!(
            plugin_dir = %self.plugin_dir,
            enabled = ?config.enabled,
            auto_migrate = config.migrations.auto_migrate,
            "Loading plugins from configuration"
        );
        
        for plugin_name in &config.enabled {
            self.load_plugin(plugin_name, config, registry).await?;
        }
        
        // Initialize all loaded plugins with their configs
        registry.initialize_all(&config.configs).await?;
        
        // Run migrations if auto_migrate is enabled
        if config.migrations.auto_migrate {
            info!("Running plugin migrations...");
            self.run_migrations(config, registry).await?;
        }
        
        Ok(())
    }
    
    /// Run migrations for all migratable plugins
    pub async fn run_migrations(
        &self,
        config: &PluginConfig,
        registry: &PluginRegistry,
    ) -> Result<()> {
        for plugin_name in &config.enabled {
            let migration_opts = config.get_migration_options(plugin_name);
            
            // Skip if auto_migrate disabled for this plugin
            if !migration_opts.auto_migrate {
                continue;
            }
            
            // Check if migrations directory exists
            if !std::path::Path::new(&migration_opts.migrations_dir).exists() {
                info!(
                    plugin = %plugin_name,
                    dir = %migration_opts.migrations_dir,
                    "No migrations directory, skipping"
                );
                continue;
            }
            
            info!(
                plugin = %plugin_name,
                dir = %migration_opts.migrations_dir,
                "Running migrations"
            );
            
            // The actual migration is handled by the plugin via MigratablePlugin trait
            // This just triggers it through the registry
            if let Err(e) = registry.run_plugin_migrations(plugin_name, &migration_opts).await {
                if migration_opts.strict_checksums {
                    return Err(PluginError::MigrationFailed(format!(
                        "Plugin {} migration failed: {}",
                        plugin_name, e
                    )));
                } else {
                    warn!(
                        plugin = %plugin_name,
                        error = %e,
                        "Migration failed (non-strict mode, continuing)"
                    );
                }
            }
        }
        
        Ok(())
    }
    
    /// Load a single plugin by name
    async fn load_plugin(
        &self,
        name: &str,
        config: &PluginConfig,
        registry: &PluginRegistry,
    ) -> Result<()> {
        info!(plugin = %name, "Loading plugin");
        
        // Check if it's a built-in plugin
        if self.load_builtin_plugin(name, config, registry).await? {
            return Ok(());
        }
        
        // Try dynamic loading
        #[cfg(feature = "dynamic-loading")]
        {
            self.load_dynamic_plugin(name, registry).await?;
        }
        
        #[cfg(not(feature = "dynamic-loading"))]
        {
            warn!(plugin = %name, "Plugin not found (dynamic loading disabled)");
        }
        
        Ok(())
    }
    
    /// Load built-in plugins (compile-time registered)
    async fn load_builtin_plugin(
        &self,
        name: &str,
        _config: &PluginConfig,
        _registry: &PluginRegistry,
    ) -> Result<bool> {
        // Built-in plugins are registered directly by the application
        // This method returns false to indicate the plugin wasn't found as built-in
        // The actual registration happens in the application's main.rs
        
        match name {
            "postgres" => {
                // PostgreSQL is the default, always available
                info!(plugin = %name, "Built-in PostgreSQL plugin (registered by application)");
                Ok(true)
            }
            _ => Ok(false)
        }
    }
    
    /// Load a plugin dynamically from a shared library
    #[cfg(feature = "dynamic-loading")]
    async fn load_dynamic_plugin(
        &self,
        name: &str,
        registry: &PluginRegistry,
    ) -> Result<()> {
        use libloading::{Library, Symbol};
        
        // Construct library path
        let lib_name = format!("libzradar_plugin_{}", name.replace("-", "_"));
        
        #[cfg(target_os = "linux")]
        let lib_path = format!("{}/{}.so", self.plugin_dir, lib_name);
        
        #[cfg(target_os = "macos")]
        let lib_path = format!("{}/{}.dylib", self.plugin_dir, lib_name);
        
        #[cfg(target_os = "windows")]
        let lib_path = format!("{}/{}.dll", self.plugin_dir, lib_name);
        
        if !Path::new(&lib_path).exists() {
            return Err(PluginError::NotFound(format!(
                "Plugin library not found: {}",
                lib_path
            )));
        }
        
        info!(plugin = %name, path = %lib_path, "Loading dynamic plugin");
        
        unsafe {
            let lib = Library::new(&lib_path)
                .map_err(|e| PluginError::LoadFailed(format!("Failed to load {}: {}", lib_path, e)))?;
            
            // Look for the register function
            let register_fn: Symbol<unsafe extern "C" fn(&PluginRegistry) -> bool> = lib
                .get(b"register_plugin")
                .map_err(|e| PluginError::LoadFailed(format!(
                    "Plugin {} missing register_plugin function: {}",
                    name, e
                )))?;
            
            // Call register function
            let success = register_fn(registry);
            
            if !success {
                return Err(PluginError::LoadFailed(format!(
                    "Plugin {} registration failed",
                    name
                )));
            }
            
            // Keep library loaded (don't drop it)
            std::mem::forget(lib);
        }
        
        info!(plugin = %name, "Dynamic plugin loaded successfully");
        Ok(())
    }
    
    /// Discover available plugins in the plugin directory
    pub fn discover_plugins(&self) -> Vec<String> {
        let mut plugins = Vec::new();
        
        let plugin_dir = Path::new(&self.plugin_dir);
        if !plugin_dir.exists() {
            warn!(path = %self.plugin_dir, "Plugin directory does not exist");
            return plugins;
        }
        
        if let Ok(entries) = std::fs::read_dir(plugin_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Extract plugin name from library filename
                    if filename.starts_with("libzradar_plugin_") {
                        let name = filename
                            .trim_start_matches("libzradar_plugin_")
                            .trim_end_matches(".so")
                            .trim_end_matches(".dylib")
                            .trim_end_matches(".dll")
                            .replace("_", "-");
                        
                        plugins.push(name);
                    }
                }
            }
        }
        
        info!(count = plugins.len(), plugins = ?plugins, "Discovered plugins");
        plugins
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new("./plugins")
    }
}

