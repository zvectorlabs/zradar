//! Plugin trait definitions
//!
//! All plugins must implement the base `Plugin` trait.
//! Specialized plugins implement additional traits based on their type.
//!
//! ## Migration System
//!
//! Plugins that manage databases can implement `MigratablePlugin` to:
//! - Run schema migrations during startup
//! - Track applied migrations
//! - Verify migration integrity

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::Result;
use zradar_traits::{BlockStorage, JobQueue};

/// Plugin type identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PluginType {
    /// Block storage (S3, GCS, Azure Blob, Local)
    Storage,
    /// Job queue (Redis, Kafka, RabbitMQ)
    Queue,
    /// Telemetry writer (ClickHouse, TimescaleDB)
    TelemetryWriter,
    /// Telemetry reader (ClickHouse, TimescaleDB)
    TelemetryReader,
    /// Cache provider (Redis, Memcached)
    Cache,
    /// Authentication provider (LDAP, OAuth2)
    Auth,
    /// Custom plugin type
    Custom(String),
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginType::Storage => write!(f, "storage"),
            PluginType::Queue => write!(f, "queue"),
            PluginType::TelemetryWriter => write!(f, "telemetry_writer"),
            PluginType::TelemetryReader => write!(f, "telemetry_reader"),
            PluginType::Cache => write!(f, "cache"),
            PluginType::Auth => write!(f, "auth"),
            PluginType::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Plugin metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMetadata {
    /// Unique plugin identifier (e.g., "clickhouse", "s3", "redis")
    pub name: String,
    /// Plugin version (semver)
    pub version: String,
    /// Plugin author
    pub author: String,
    /// Plugin description
    pub description: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Plugin dependencies (other plugin names)
    pub dependencies: Vec<String>,
    /// Supported configuration keys
    pub config_schema: Vec<ConfigField>,
}

/// Configuration field schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigField {
    /// Field name
    pub name: String,
    /// Field description
    pub description: String,
    /// Is this field required?
    pub required: bool,
    /// Default value (if any)
    pub default: Option<serde_json::Value>,
    /// Field type hint
    pub field_type: String,
}

/// Base plugin trait - all plugins must implement this
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Get plugin metadata
    fn metadata(&self) -> &PluginMetadata;

    /// Validate configuration before initialization
    fn validate_config(&self, config: &serde_json::Value) -> Result<()>;

    /// Initialize the plugin with configuration
    async fn initialize(&self, config: &serde_json::Value) -> Result<()>;

    /// Health check
    async fn health_check(&self) -> Result<bool>;

    /// Shutdown the plugin gracefully
    async fn shutdown(&self) -> Result<()>;
}

/// Storage plugin trait
#[async_trait]
pub trait StoragePlugin: Plugin {
    /// Create a block storage instance
    async fn create_storage(&self, config: &serde_json::Value) -> Result<Arc<dyn BlockStorage>>;
}

/// Queue plugin trait
#[async_trait]
pub trait QueuePlugin: Plugin {
    /// Create a job queue instance
    async fn create_queue(&self, config: &serde_json::Value) -> Result<Arc<dyn JobQueue>>;
}

/// Telemetry writer plugin trait
///
/// Note: Uses anyhow::Result to avoid dependency on zradar-control errors
#[async_trait]
pub trait TelemetryWriterPlugin: Plugin {
    /// Insert spans
    async fn insert_spans(&self, spans: &[zradar_models::Span]) -> anyhow::Result<()>;

    /// Insert metrics
    async fn insert_metrics(&self, metrics: &[zradar_models::Metric]) -> anyhow::Result<()>;
}

/// Telemetry reader plugin trait
///
/// Note: Returns raw JSON to avoid type coupling
#[async_trait]
pub trait TelemetryReaderPlugin: Plugin {
    /// Query traces with filters (returns JSON)
    async fn query_traces(&self, filters: serde_json::Value) -> anyhow::Result<serde_json::Value>;

    /// Get trace detail (returns JSON)
    async fn get_trace_detail(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>>;

    /// Query spans (returns JSON)
    async fn query_spans(&self, filters: serde_json::Value) -> anyhow::Result<serde_json::Value>;

    /// Get analytics (returns JSON)
    async fn get_analytics(&self, query: serde_json::Value) -> anyhow::Result<serde_json::Value>;
}

/// Cache plugin trait
#[async_trait]
pub trait CachePlugin: Plugin {
    /// Get value from cache
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;

    /// Set value in cache with TTL
    async fn set(&self, key: &str, value: &[u8], ttl_seconds: Option<u64>) -> anyhow::Result<()>;

    /// Delete value from cache
    async fn delete(&self, key: &str) -> anyhow::Result<()>;

    /// Check if key exists
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;
}

// =============================================================================
// Migration System
// =============================================================================

/// Applied migration record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    /// Migration name/version (e.g., "001_initial_schema")
    pub name: String,
    /// SHA256 checksum of migration content
    pub checksum: String,
    /// When the migration was applied
    pub applied_at: chrono::DateTime<chrono::Utc>,
    /// Duration to apply (milliseconds)
    pub duration_ms: u64,
}

/// Migration status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// All migrations applied
    UpToDate,
    /// Pending migrations to apply
    Pending { count: usize, names: Vec<String> },
    /// Checksum mismatch detected
    ChecksumMismatch { migration: String },
    /// Error checking status
    Error(String),
}

/// Migration options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationOptions {
    /// Directory containing migration files
    pub migrations_dir: String,
    /// Run migrations automatically on startup
    pub auto_migrate: bool,
    /// Fail if checksum mismatch detected
    pub strict_checksums: bool,
    /// Dry run (show what would be applied without applying)
    pub dry_run: bool,
}

/// Migratable plugin trait
///
/// Implement this for plugins that manage database schemas.
///
/// ## Migration File Format
///
/// Migrations are SQL files named with version prefix:
/// ```text
/// migrations/
///   001_initial_schema.sql
///   002_add_indexes.sql
///   003_add_partitions.sql
/// ```
///
/// ## Example
///
/// ```ignore
/// #[async_trait]
/// impl MigratablePlugin for ClickHousePlugin {
///     async fn run_migrations(&self, options: &MigrationOptions) -> anyhow::Result<Vec<AppliedMigration>> {
///         let client = self.get_client().await?;
///         client.run_migrations(&options.migrations_dir).await
///     }
///     
///     async fn migration_status(&self, options: &MigrationOptions) -> anyhow::Result<MigrationStatus> {
///         // Check pending migrations
///     }
/// }
/// ```
#[async_trait]
pub trait MigratablePlugin: Plugin {
    /// Run pending migrations
    ///
    /// Returns list of applied migrations in order.
    async fn run_migrations(
        &self,
        options: &MigrationOptions,
    ) -> anyhow::Result<Vec<AppliedMigration>>;

    /// Get migration status without applying
    async fn migration_status(&self, options: &MigrationOptions)
    -> anyhow::Result<MigrationStatus>;

    /// Get list of applied migrations
    async fn applied_migrations(&self) -> anyhow::Result<Vec<AppliedMigration>>;

    /// Verify all applied migration checksums
    async fn verify_checksums(&self, options: &MigrationOptions) -> anyhow::Result<bool>;

    /// Rollback last N migrations (if supported)
    ///
    /// Not all databases support rollback. Returns error if not supported.
    async fn rollback(&self, _count: usize) -> anyhow::Result<()> {
        anyhow::bail!("Rollback not supported by this plugin")
    }
}

/// Score storage plugin trait (for evaluation scores)
///
/// Plugins that store evaluation/scoring data implement this.
#[async_trait]
pub trait ScoreStoragePlugin: Plugin + MigratablePlugin {
    /// Insert evaluation scores
    async fn insert_scores(&self, scores: &[zradar_models::EvaluationScore]) -> anyhow::Result<()>;

    /// Get scores for a trace
    async fn get_trace_scores(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Vec<zradar_models::EvaluationScore>>;

    /// Get scores for a session
    async fn get_session_scores(
        &self,
        project_id: uuid::Uuid,
        session_id: &str,
    ) -> anyhow::Result<Vec<zradar_models::EvaluationScore>>;

    /// Get score summary for a trace
    async fn get_trace_score_summary(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>>;

    /// Get a specific score by ID
    async fn get_score_by_id(
        &self,
        project_id: uuid::Uuid,
        score_id: uuid::Uuid,
    ) -> anyhow::Result<Option<zradar_models::EvaluationScore>>;

    /// Soft delete a score
    async fn soft_delete_score(
        &self,
        project_id: uuid::Uuid,
        score_id: uuid::Uuid,
    ) -> anyhow::Result<bool>;
}

/// Plugin factory function type (for dynamic loading)
pub type PluginFactory = fn() -> Box<dyn Plugin>;

/// Storage plugin factory
pub type StoragePluginFactory = fn() -> Box<dyn StoragePlugin>;

/// Queue plugin factory
pub type QueuePluginFactory = fn() -> Box<dyn QueuePlugin>;

/// Telemetry writer plugin factory
pub type TelemetryWriterPluginFactory = fn() -> Box<dyn TelemetryWriterPlugin>;

/// Telemetry reader plugin factory
pub type TelemetryReaderPluginFactory = fn() -> Box<dyn TelemetryReaderPlugin>;
