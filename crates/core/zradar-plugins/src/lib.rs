//! # zradar-plugins
//!
//! Plugin system for zradar architecture.
//!
//! ## Overview
//!
//! This crate provides:
//! - Plugin trait definitions
//! - Plugin registry (runtime discovery and management)
//! - Plugin loader (dynamic .so/.dylib loading)
//! - Configuration-driven plugin initialization
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                          PLUGIN SYSTEM                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │   ┌───────────────┐    ┌───────────────┐    ┌───────────────┐ │
//! │   │   Registry    │◄───│    Loader     │◄───│  Config       │ │
//! │   │  (singleton)  │    │ (dlopen/.so)  │    │ (plugins.toml)│ │
//! │   └───────┬───────┘    └───────────────┘    └───────────────┘ │
//! │           │                                                    │
//! │           ▼                                                    │
//! │   ┌───────────────────────────────────────────────────────┐   │
//! │   │                Plugin Implementations                  │   │
//! │   │  StoragePlugin │ QueuePlugin │ TelemetryPlugin │ ...  │   │
//! │   └───────────────────────────────────────────────────────┘   │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod error;
pub mod loader;
pub mod plugin;
pub mod registry;

// Re-export key types
pub use config::PluginConfig;
pub use error::PluginError;
pub use loader::PluginLoader;
pub use plugin::*;
pub use registry::PluginRegistry;

/// Global plugin registry instance
use std::sync::OnceLock;
static GLOBAL_REGISTRY: OnceLock<PluginRegistry> = OnceLock::new();

/// Get the global plugin registry
pub fn global_registry() -> &'static PluginRegistry {
    GLOBAL_REGISTRY.get_or_init(PluginRegistry::new)
}
