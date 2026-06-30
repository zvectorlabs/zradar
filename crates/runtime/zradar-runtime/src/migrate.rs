//! Standalone database migration helper.
//!
//! Exposes [`migrate`] as a first-class async function so callers can run all
//! pending migrations and exit without starting the full server. This is useful
//! for Kubernetes Jobs, init containers, or any deployment that needs a
//! migrate-then-serve pattern rather than auto-migrate on startup.
//!
//! [`ZradarRuntimeBuilder::run`] continues to call migrations inline as a
//! safety net for simpler deployments — this module is an additive API, not a
//! replacement.

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use zradar_plugin_postgres::migrations::MIGRATIONS;

/// Run all pending database migrations against `database_url`.
///
/// Opens a small connection pool (max 2 connections), applies every pending
/// migration from [`zradar_plugin_postgres::migrations::MIGRATIONS`], then
/// closes the pool and returns.
///
/// # Errors
///
/// Returns an error if the database is unreachable or any migration fails.
///
/// # Example
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let database_url = std::env::var("DATABASE_URL")?;
/// zradar_runtime::migrate(&database_url).await?;
/// # Ok(())
/// # }
/// ```
pub async fn migrate(database_url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await?;
    MIGRATIONS.run(&pool).await?;
    Ok(())
}
