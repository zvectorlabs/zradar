//! Configuration management

mod auth;
mod cors;
mod database;
mod ingestor;
mod loader;

pub use auth::{ApiKeyConfig, AuthConfig};
pub use cors::CorsConfig;
pub use database::PostgresConfig;
pub use ingestor::{IngestorConfig, ParquetStorageConfig, S3StorageConfig, StorageConfig};
pub use loader::Config;
