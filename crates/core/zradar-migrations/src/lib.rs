//! # zradar-migrations
//!
//! Centralized migration registry system for zradar plugins.
//!
//! This crate provides:
//! - `MigrationRegistry`: Central registry that tracks all plugin migrations in PostgreSQL
//! - `MigrationProvider`: Trait that plugins implement to provide migration discovery and execution
//! - Auto-migration on startup with single configuration flag
//! - Multi-database support (PostgreSQL, ClickHouse, etc.)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │           zradar-server/worker                  │
//! │  ┌──────────────────────────────────────────┐   │
//! │  │      MigrationRegistry::new(pool)        │   │
//! │  │  registry.register_plugin(postgres_prov) │   │
//! │  │  registry.register_plugin(clickhouse_prov│   │
//! │  │  registry.run_all_migrations()           │   │
//! │  └──────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────┘
//!                          │
//!          ┌───────────────┼───────────────┐
//!          ▼               ▼               ▼
//!   ┌──────────┐    ┌──────────┐    ┌──────────┐
//!   │ Postgres │    │ClickHouse│    │  Custom  │
//!   │ Provider │    │ Provider │    │ Provider │
//!   └──────────┘    └──────────┘    └──────────┘
//!          │               │               │
//!          └───────────────┴───────────────┘
//!                          │
//!                          ▼
//!              ┌────────────────────────┐
//!              │ PostgreSQL Tracking DB │
//!              │  _plugin_migrations    │
//!              └────────────────────────┘
//! ```

mod provider;
mod registry;
mod types;

pub use provider::MigrationProvider;
pub use registry::MigrationRegistry;
pub use types::*;
