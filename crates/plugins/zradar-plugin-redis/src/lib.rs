//! # zradar-plugin-redis
//!
//! Redis plugin for zradar - provides cache and hybrid queue functionality.
//!
//! ## Features
//!
//! - **Cache**: Fast key-value caching with TTL
//! - **Hybrid Queue**: Redis (coordination) + PostgreSQL (durability)
//!   - Ultra-fast dequeue via Redis BLPOP
//!   - PostgreSQL as source of truth
//!   - Supports 1000+ workers, 100K+ jobs/sec

mod cache;
mod plugin;
mod hybrid_queue;

pub use cache::RedisCache;
pub use plugin::RedisPlugin;
pub use hybrid_queue::HybridQueue;

use std::sync::Arc;
use zradar_plugins::PluginRegistry;

/// Register plugin (for dynamic loading)
#[unsafe(no_mangle)]
pub extern "C" fn register_plugin(registry: &PluginRegistry) -> bool {
    let plugin = Arc::new(RedisPlugin::new());
    
    if let Err(e) = registry.register_cache(plugin) {
        tracing::error!(error = %e, "Failed to register Redis plugin");
        return false;
    }
    
    tracing::info!("Redis plugin registered successfully");
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

