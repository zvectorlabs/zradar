//! # zradar-plugin-s3
//!
//! S3 storage plugin for zradar block storage.

mod storage;
mod plugin;

pub use storage::S3BlockStorage;
pub use plugin::S3Plugin;

use std::sync::Arc;
use zradar_plugins::PluginRegistry;

/// Register plugin (for dynamic loading)
#[unsafe(no_mangle)]
pub extern "C" fn register_plugin(registry: &PluginRegistry) -> bool {
    let plugin = Arc::new(S3Plugin::new());
    
    if let Err(e) = registry.register_storage(plugin) {
        tracing::error!(error = %e, "Failed to register S3 plugin");
        return false;
    }
    
    tracing::info!("S3 plugin registered successfully");
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

