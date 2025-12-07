//! # zradar-plugin-local
//!
//! Local filesystem storage plugin for development and testing.
//!
//! ## Use Cases
//!
//! - Development environments
//! - Single-node deployments
//! - Testing without cloud dependencies
//!
//! ## Note
//!
//! For production multi-node deployments, use `zradar-plugin-s3` instead.

mod plugin;
mod storage;

pub use plugin::LocalStoragePlugin;
pub use storage::LocalBlockStorage;

// /// Register this plugin with the registry (for dynamic loading)
// /// Note: Disabled to avoid symbol conflicts when statically linking multiple plugins
// #[unsafe(no_mangle)]
// pub extern "C" fn register_plugin(registry: &PluginRegistry) -> bool {
//     let plugin = Arc::new(LocalStoragePlugin::new());
//
//     if let Err(e) = registry.register_storage(plugin) {
//         tracing::error!("Failed to register local storage plugin: {}", e);
//         return false;
//     }
//
//     tracing::info!("Registered local storage plugin");
//     true
// }

/// Get plugin metadata (for static linking)
pub fn local_plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
