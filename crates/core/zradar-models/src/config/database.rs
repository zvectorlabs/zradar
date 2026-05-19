//! Database configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

// Default functions
fn default_max_connections() -> usize {
    10
}
