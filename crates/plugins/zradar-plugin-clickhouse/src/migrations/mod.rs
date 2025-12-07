//! ClickHouse migrations module

pub mod provider;

// Legacy exports for compatibility
pub use zradar_migrations::MigrationInfo as MigrationResult;
pub type MigrationRunner = (); // Stub
pub type MigrationError = anyhow::Error;
