//! # zradar-plugin-postgres
//!
//! PostgreSQL plugin - the default implementation for all repositories.
//!
//! ## Features
//!
//! - **Full repository implementations**: Users, Organizations, Projects, API Keys, Roles
//! - **Telemetry storage**: Spans, Metrics (for smaller deployments)
//! - **Score storage**: Evaluation scores
//! - **Job queue**: PostgreSQL-based queue (up to 50 workers)
//! - **Audit logging**: Full audit trail
//! - **Embedded migrations**: All schema migrations bundled in binary
//!
//! ## Architecture
//!
//! This plugin implements all traits from `zradar-traits`, making PostgreSQL
//! the default "batteries included" backend for zradar.
//!
//! For high-scale telemetry (1000+ workers, millions of spans/day),
//! use `zradar-plugin-clickhouse` instead.

pub mod client;
pub mod compression;
pub mod migrations;
pub mod plugin;
pub mod repositories;

pub use client::PostgresClient;
pub use plugin::PostgresPlugin;

// Re-export individual repositories for direct use
pub use repositories::{
    PostgresApiKeyRepository, PostgresAuditLogger, PostgresFileListRepository, PostgresJobQueue,
    PostgresOrganizationRepository, PostgresProjectRepository, PostgresRoleRepository,
    PostgresScoreRepository, PostgresTelemetryRepository, PostgresUserRepository,
};

// /// Register this plugin with the registry (for dynamic loading)
// /// Note: Disabled to avoid symbol conflicts when statically linking multiple plugins
// #[unsafe(no_mangle)]
// pub extern "C" fn register_plugin(registry: &PluginRegistry) -> bool {
//     let plugin = Arc::new(PostgresPlugin::new());
//
//     // PostgreSQL is the default for everything
//     tracing::info!("Registering PostgreSQL plugin as default implementation");
//
//     true
// }

/// Get plugin metadata (for static linking)
pub fn postgres_plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
