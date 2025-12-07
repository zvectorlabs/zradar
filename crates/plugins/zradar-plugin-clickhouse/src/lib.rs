//! # zradar-plugin-clickhouse
//!
//! ClickHouse plugin for zradar telemetry storage and analytics.
//!
//! ## Features
//!
//! - High-performance telemetry writes (spans, metrics)
//! - Optimized telemetry reads (queries, analytics)
//! - Migration support
//! - Test mode (synchronous mutations)
//!
//! ## Usage
//!
//! ### Static linking (compile-time)
//! ```ignore
//! use zradar_plugin_clickhouse::ClickHousePlugin;
//!
//! let plugin = ClickHousePlugin::new();
//! registry.register_writer(Arc::new(plugin))?;
//! ```
//!
//! ### Dynamic loading (runtime)
//! Plugin is automatically registered when loaded via `dlopen`.

mod client;
mod migrations;
mod plugin;
mod reader;
mod score_impl;
mod writer;

pub use client::{ClickHouseClient, ScoreSummary};
pub use migrations::{MigrationError, MigrationResult, MigrationRunner};
pub use plugin::ClickHousePlugin;
pub use reader::ClickHouseTelemetryReader;
pub use writer::ClickHouseTelemetryWriter;

use std::sync::Arc;
use zradar_plugins::PluginRegistry;

/// Register this plugin with the registry (for dynamic loading)
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn register_plugin(registry: &PluginRegistry) -> bool {
    let plugin = Arc::new(ClickHousePlugin::new());

    // Register as both writer and reader
    if let Err(e) = registry.register_writer(plugin.clone()) {
        tracing::error!(error = %e, "Failed to register ClickHouse writer plugin");
        return false;
    }

    if let Err(e) = registry.register_reader(plugin) {
        tracing::error!(error = %e, "Failed to register ClickHouse reader plugin");
        return false;
    }

    tracing::info!("ClickHouse plugin registered successfully");
    true
}

/// Get plugin metadata (for dynamic loading)
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
