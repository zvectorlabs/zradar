//! Database configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,

    #[serde(default = "default_user")]
    pub user: String,

    #[serde(default)]
    pub password: String,

    #[serde(default = "default_database")]
    pub database: String,

    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

// Default functions
fn default_user() -> String {
    "default".to_string()
}
fn default_database() -> String {
    "telemetry".to_string()
}
fn default_max_connections() -> usize {
    10
}
