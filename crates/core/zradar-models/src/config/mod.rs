//! Configuration management

mod auth;
mod database;
mod ingestor;
mod loader;
mod migrations;

// Re-export all config types
pub use auth::{AdminApiConfig, ApiKeyConfig, AuthConfig};
pub use database::{ClickHouseConfig, PostgresConfig};
pub use ingestor::{
    IngestorConfig, LocalStorageConfig, RedisConfig, S3StorageConfig, StorageConfig, WorkersConfig,
};
pub use loader::Config;
pub use migrations::MigrationsConfig;
