//! Migration types and data structures

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Plugin migration tracked in PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PluginMigration {
    pub id: Uuid,
    pub plugin_name: String,
    pub plugin_version: String,
    pub migration_version: String,
    pub migration_name: String,
    pub checksum: String,
    pub applied_at: DateTime<Utc>,
    pub execution_time_ms: i32,
    pub status: String,
    pub error_message: Option<String>,
    pub migration_type: String,
    #[sqlx(json)]
    pub metadata: serde_json::Value,
}

/// Migration information discovered from plugin
#[derive(Debug, Clone)]
pub struct MigrationInfo {
    pub version: String,
    pub name: String,
    pub checksum: String,
    pub content: String,
}

/// Result of applying a migration
#[derive(Debug)]
pub struct MigrationResult {
    pub success: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Type of migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationType {
    Postgres,
    ClickHouse,
    Custom(String),
}

impl MigrationType {
    pub fn as_str(&self) -> &str {
        match self {
            MigrationType::Postgres => "postgres",
            MigrationType::ClickHouse => "clickhouse",
            MigrationType::Custom(s) => s.as_str(),
        }
    }
}

/// Summary of migration run across all plugins
#[derive(Debug, Default, Clone, Serialize)]
pub struct MigrationSummary {
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
    pub plugin_results: Vec<PluginMigrationResult>,
}

/// Migration result for a specific plugin
#[derive(Debug, Clone, Serialize)]
pub struct PluginMigrationResult {
    pub plugin_name: String,
    pub migrations_applied: usize,
    pub duration_ms: u64,
    pub status: String, // "success", "failed", "skipped"
}

/// Migration status for a plugin
#[derive(Debug, Clone, Serialize)]
pub struct PluginMigrationStatus {
    pub plugin_name: String,
    pub plugin_version: String,
    pub applied_count: usize,
    pub pending_count: usize,
    pub last_migration: Option<String>,
    pub last_applied_at: Option<DateTime<Utc>>,
}
