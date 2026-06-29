//! Configuration loading from TOML and environment

use super::*;
use anyhow::Result;
use serde::Deserialize;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_otlp_port")]
    pub otlp_port: u16,

    /// OTLP/HTTP receiver port (default: 4318). Set to 0 to disable.
    #[serde(default = "default_otlp_http_port")]
    pub otlp_http_port: u16,

    #[serde(default = "default_query_api_port")]
    pub query_api_port: u16,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_seconds: u64,

    #[serde(default)]
    pub postgres: Option<PostgresConfig>,

    /// API keys for config-based authentication.
    #[serde(default)]
    pub api_keys: Vec<ApiKeyConfig>,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub ingestor: Option<IngestorConfig>,

    /// Admin API port (defaults to `query_api_port`).
    #[serde(default)]
    pub admin_api_port: Option<u16>,

    /// Port for the Query gRPC API (default: 8081).
    #[serde(default)]
    pub query_grpc_port: Option<u16>,

    /// Port for the Admin gRPC API (default: 8082).
    #[serde(default)]
    pub admin_grpc_port: Option<u16>,
}

impl Config {
    /// Load configuration from `config.toml` or fall back to environment variables.
    pub fn load() -> Result<Self> {
        let mut config = if let Ok(contents) = std::fs::read_to_string("config.toml") {
            toml::from_str(&contents)?
        } else {
            Config {
                otlp_port: std::env::var("OTLP_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4317),
                otlp_http_port: std::env::var("OTLP_HTTP_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4318),
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
                postgres: None,
                api_keys: Vec::new(),
                auth: AuthConfig::default(),
                ingestor: None,
                admin_api_port: None,
                query_grpc_port: None,
                admin_grpc_port: None,
            }
        };

        // Port overrides from environment
        if let Some(p) = std::env::var("OTLP_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
        {
            config.otlp_port = p;
        }
        if let Some(p) = std::env::var("QUERY_API_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
        {
            config.query_api_port = p;
        }

        Ok(config)
    }

    /// Effective admin API port (falls back to query_api_port).
    pub fn effective_admin_port(&self) -> u16 {
        self.admin_api_port.unwrap_or(self.query_api_port)
    }

    /// Effective Query gRPC port (default: 8081).
    pub fn effective_query_grpc_port(&self) -> u16 {
        self.query_grpc_port.unwrap_or(8081)
    }

    /// Effective Admin gRPC port (default: 8082).
    pub fn effective_admin_grpc_port(&self) -> u16 {
        self.admin_grpc_port.unwrap_or(8082)
    }
}

fn default_otlp_port() -> u16 {
    4317
}
fn default_otlp_http_port() -> u16 {
    4318
}
fn default_query_api_port() -> u16 {
    8080
}
fn default_batch_size() -> usize {
    1000
}
fn default_batch_timeout() -> u64 {
    10
}
