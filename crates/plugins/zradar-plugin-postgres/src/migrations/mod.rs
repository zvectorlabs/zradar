//! Embedded PostgreSQL migrations

/// Embedded migrations — run with `MIGRATIONS.run(&pool).await?`
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
