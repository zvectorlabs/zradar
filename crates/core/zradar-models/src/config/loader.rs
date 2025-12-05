//! Configuration loading from TOML and environment

use serde::Deserialize;
use anyhow::Result;
use super::*;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_otlp_port")]
    pub otlp_port: u16,
    
    #[serde(default = "default_query_api_port")]
    pub query_api_port: u16,
    
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_seconds: u64,
    
    pub clickhouse: ClickHouseConfig,
    
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,
    
    #[serde(default)]
    pub admin_api: Option<AdminApiConfig>,
    
    #[serde(default)]
    pub auth: AuthConfig,
    
    #[serde(default)]
    pub ingestor: Option<IngestorConfig>,
    
    #[serde(default)]
    pub workers: Option<WorkersConfig>,
    
    #[serde(default)]
    pub migrations: Option<MigrationsConfig>,
}

impl Config {
    /// Load configuration from file or environment
    pub fn load() -> Result<Self> {
        // Try to load from config.toml
        let mut config = if let Ok(contents) = std::fs::read_to_string("config.toml") {
            toml::from_str(&contents)?
        } else {
            // Fallback to environment variables
            Config {
                otlp_port: std::env::var("OTLP_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4317),
                query_api_port: std::env::var("QUERY_API_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(8080),
                batch_size: std::env::var("BATCH_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
                batch_timeout_seconds: std::env::var("BATCH_TIMEOUT_SECONDS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10),
                clickhouse: ClickHouseConfig {
                    url: std::env::var("CLICKHOUSE_URL")
                        .unwrap_or_else(|_| "http://localhost:8123".to_string()),
                    user: std::env::var("CLICKHOUSE_USER")
                        .unwrap_or_else(|_| "default".to_string()),
                    password: std::env::var("CLICKHOUSE_PASSWORD")
                        .unwrap_or_default(),
                    database: std::env::var("CLICKHOUSE_DATABASE")
                        .unwrap_or_else(|_| "telemetry".to_string()),
                    max_connections: std::env::var("CLICKHOUSE_MAX_CONNECTIONS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10),
                },
                postgres: None,
                admin_api: None,
                auth: AuthConfig::default(),
                ingestor: None,
                workers: None,
                migrations: None,
            }
        };
        
        // Override migration settings from environment variables
        if let Some(ref mut migrations) = config.migrations {
            if let Ok(val) = std::env::var("AUTO_MIGRATE") {
                migrations.auto_migrate = val.to_lowercase() == "true" || val == "1";
            }
        } else if std::env::var("AUTO_MIGRATE").is_ok() {
            let mut migrations = MigrationsConfig::default();
            if let Ok(val) = std::env::var("AUTO_MIGRATE") {
                migrations.auto_migrate = val.to_lowercase() == "true" || val == "1";
            }
            config.migrations = Some(migrations);
        }
        
        Ok(config)
    }
}

// Default functions
fn default_otlp_port() -> u16 { 4317 }
fn default_query_api_port() -> u16 { 8080 }
fn default_batch_size() -> usize { 1000 }
fn default_batch_timeout() -> u64 { 10 }

