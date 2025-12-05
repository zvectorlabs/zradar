//! Embedded PostgreSQL migrations
//!
//! Migrations are embedded in the binary using sqlx's migrate! macro.
//! They are automatically run when the plugin initializes via the MigrationRegistry.

pub mod provider;

pub use provider::PostgresMigrationProvider;

/// Embedded migrations
/// 
/// Usage:
/// ```ignore
/// use zradar_plugin_postgres::migrations::MIGRATIONS;
/// 
/// MIGRATIONS.run(&pool).await?;
/// ```
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

