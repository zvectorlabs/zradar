//! # zradar-plugin-postgres
//!
//! PostgreSQL backend for zradar.
//! Provides file_list and stream_stats repository implementations.

pub mod client;
pub mod migrations;
pub mod repositories;

pub use client::PostgresClient;
pub use repositories::{
    PostgresAuditLogRepository, PostgresFileListRepository, PostgresRetentionPolicyRepository,
    PostgresSettingsRepository,
};

/// Crate version
pub fn postgres_plugin_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
