//! ClickHouse migrations module

pub mod provider;

#[allow(unused_imports)]
pub use provider::ClickHouseMigrationProvider;

// Legacy exports for compatibility
pub use zradar_migrations::MigrationInfo as MigrationResult;
pub type MigrationRunner = (); // Stub
pub type MigrationError = anyhow::Error;
