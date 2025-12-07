//! Migration provider trait
//!
//! Plugins implement this trait to provide migration discovery and execution

use super::types::{MigrationInfo, MigrationResult, MigrationType};

/// Trait that plugins implement to provide migration capabilities
pub trait MigrationProvider: Send + Sync {
    /// Plugin name (e.g., "postgres", "clickhouse")
    fn plugin_name(&self) -> &str;

    /// Plugin version
    fn plugin_version(&self) -> &str;

    /// Migration type
    fn migration_type(&self) -> MigrationType;

    /// Get embedded migrations directory (if any)
    fn migrations_dir(&self) -> Option<&str>;

    /// Discover all migrations (pending and applied)
    fn discover_migrations(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Vec<MigrationInfo>>> + Send + '_>,
    >;

    /// Apply a specific migration
    fn apply_migration<'a>(
        &'a self,
        migration: &'a MigrationInfo,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<MigrationResult>> + Send + 'a>,
    >;

    /// Verify migration checksum
    fn verify_migration<'a>(
        &'a self,
        migration: &'a MigrationInfo,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>;
}
