//! Configuration management

mod database;
mod auth;
mod ingestor;
mod migrations;
mod loader;

// Re-export all config types
pub use database::{ClickHouseConfig, PostgresConfig};
pub use auth::{AuthConfig, ApiKeyConfig, AdminApiConfig};
pub use ingestor::{
    IngestorConfig, WorkersConfig, StorageConfig, 
    LocalStorageConfig, S3StorageConfig, RedisConfig
};
pub use migrations::MigrationsConfig;
pub use loader::Config;

