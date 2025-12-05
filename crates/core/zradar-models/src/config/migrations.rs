//! Migration configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MigrationsConfig {
    /// Enable automatic migrations on startup
    /// - On first run (no PostgreSQL setup), migrations always run
    /// - On subsequent runs, only runs if this flag is true
    #[serde(default = "default_auto_migrate")]
    pub auto_migrate: bool,
}

fn default_auto_migrate() -> bool {
    true
}

impl Default for MigrationsConfig {
    fn default() -> Self {
        Self {
            auto_migrate: true,
        }
    }
}

