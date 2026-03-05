//! Ingestor and worker configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct IngestorConfig {
    #[serde(default = "default_queue_type")]
    pub queue_type: String, // "postgres" or "hybrid"

    #[serde(default = "default_storage_type")]
    pub storage_type: String, // "local" or "s3"

    /// Skip job queue and directly write to persistence layer
    /// Useful for development/testing or low-volume deployments
    #[serde(default)]
    pub skip_job: bool,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub redis: Option<RedisConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub local: Option<LocalStorageConfig>,

    #[serde(default)]
    pub s3: Option<S3StorageConfig>,

    /// Root directory for Parquet files written by the direct-write path.
    ///
    /// Files are written under `{parquet_data_dir}/files/{tenant}/...`
    /// Default: `"./data/parquet-files"`
    #[serde(default = "default_parquet_data_dir")]
    pub parquet_data_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LocalStorageConfig {
    #[serde(default = "default_local_storage_path")]
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    #[serde(default = "default_redis_url")]
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkersConfig {
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,
}

// Default functions
fn default_queue_type() -> String {
    "postgres".to_string()
}
fn default_storage_type() -> String {
    "local".to_string()
}
fn default_local_storage_path() -> String {
    "./data/trace-batches".to_string()
}
fn default_redis_url() -> String {
    "redis://localhost:6379".to_string()
}
fn default_num_workers() -> usize {
    8
}
fn default_parquet_data_dir() -> String {
    "./data/parquet-files".to_string()
}
