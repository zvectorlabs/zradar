//! ClickHouse plugin implementation

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use zradar_models::{EvaluationScore, Metric, Span};
use zradar_plugins::{
    AppliedMigration, ConfigField, MigratablePlugin, MigrationOptions, MigrationStatus, Plugin,
    PluginMetadata, PluginType, ScoreStoragePlugin, TelemetryReaderPlugin, TelemetryWriterPlugin,
    error::{PluginError, Result},
};
use zradar_traits::TelemetryWriter; // Import trait to use its methods

use crate::client::{ClickHouseClient, SharedClickHouseClient};
use crate::reader::ClickHouseTelemetryReader;
use crate::writer::ClickHouseTelemetryWriter;

/// ClickHouse plugin - implements telemetry writer and reader
pub struct ClickHousePlugin {
    metadata: PluginMetadata,
    client: SharedClickHouseClient,
    writer: RwLock<Option<ClickHouseTelemetryWriter>>,
    reader: RwLock<Option<ClickHouseTelemetryReader>>,
}

impl ClickHousePlugin {
    /// Create a new ClickHouse plugin
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                name: "clickhouse".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "zradar".to_string(),
                description: "ClickHouse plugin for high-performance telemetry storage".to_string(),
                plugin_type: PluginType::TelemetryWriter,
                dependencies: vec![],
                config_schema: vec![
                    ConfigField {
                        name: "url".to_string(),
                        description: "ClickHouse server URL".to_string(),
                        required: true,
                        default: None,
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "user".to_string(),
                        description: "ClickHouse username".to_string(),
                        required: false,
                        default: Some(serde_json::json!("default")),
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "password".to_string(),
                        description: "ClickHouse password".to_string(),
                        required: false,
                        default: Some(serde_json::json!("")),
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "database".to_string(),
                        description: "ClickHouse database name".to_string(),
                        required: false,
                        default: Some(serde_json::json!("default")),
                        field_type: "string".to_string(),
                    },
                    ConfigField {
                        name: "test_mode".to_string(),
                        description: "Enable synchronous mutations for testing".to_string(),
                        required: false,
                        default: Some(serde_json::json!(false)),
                        field_type: "boolean".to_string(),
                    },
                ],
            },
            client: SharedClickHouseClient::new(),
            writer: RwLock::new(None),
            reader: RwLock::new(None),
        }
    }

    /// Get the internal client (for migrations, etc.)
    pub async fn get_client(&self) -> Option<Arc<ClickHouseClient>> {
        self.client.get().await
    }
}

impl Default for ClickHousePlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Plugin for ClickHousePlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.get("url").and_then(|v| v.as_str()).is_none() {
            return Err(PluginError::InvalidConfig(
                "ClickHouse 'url' is required".to_string(),
            ));
        }
        Ok(())
    }

    async fn initialize(&self, config: &serde_json::Value) -> Result<()> {
        tracing::info!("Initializing ClickHouse plugin");

        self.client
            .initialize(config)
            .await
            .map_err(|e| PluginError::InitializationFailed(e.to_string()))?;

        let client = self.client.get().await.ok_or_else(|| {
            PluginError::InitializationFailed("Client not initialized".to_string())
        })?;

        // Create writer and reader
        *self.writer.write().await = Some(ClickHouseTelemetryWriter::new(client.clone()));
        *self.reader.write().await = Some(ClickHouseTelemetryReader::new(client));

        tracing::info!("ClickHouse plugin initialized successfully");
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        if let Some(client) = self.client.get().await {
            client
                .health_check()
                .await
                .map_err(|e| PluginError::OperationFailed(e.to_string()))
        } else {
            Ok(false)
        }
    }

    async fn shutdown(&self) -> Result<()> {
        tracing::info!("Shutting down ClickHouse plugin");

        *self.writer.write().await = None;
        *self.reader.write().await = None;
        self.client.shutdown().await;

        Ok(())
    }
}

#[async_trait]
impl TelemetryWriterPlugin for ClickHousePlugin {
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        let writer = self.writer.read().await;
        let writer = writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        writer.insert_spans(spans).await
    }

    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        let writer = self.writer.read().await;
        let writer = writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        writer.insert_metrics(metrics).await
    }
}

#[async_trait]
impl TelemetryReaderPlugin for ClickHousePlugin {
    async fn query_traces(&self, filters: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let reader = self.reader.read().await;
        let reader = reader
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        reader.query_traces(filters).await
    }

    async fn get_trace_detail(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let reader = self.reader.read().await;
        let reader = reader
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        reader.get_trace_detail(project_id, trace_id).await
    }

    async fn query_spans(&self, filters: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let reader = self.reader.read().await;
        let reader = reader
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        reader.query_spans(filters).await
    }

    async fn get_analytics(&self, query: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let reader = self.reader.read().await;
        let reader = reader
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse plugin not initialized"))?;

        reader.get_analytics(query).await
    }
}

// =============================================================================
// Migration Support
// =============================================================================

#[async_trait]
impl MigratablePlugin for ClickHousePlugin {
    async fn run_migrations(
        &self,
        options: &MigrationOptions,
    ) -> anyhow::Result<Vec<AppliedMigration>> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        tracing::info!(
            migrations_dir = %options.migrations_dir,
            "Running ClickHouse migrations"
        );

        // Use the internal migration runner
        let _result = client.run_migrations(&options.migrations_dir).await?;

        // Convert internal migration results to plugin format
        // For now, return empty vec - migrations are tracked internally
        Ok(vec![])
    }

    async fn migration_status(
        &self,
        options: &MigrationOptions,
    ) -> anyhow::Result<MigrationStatus> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        // Check if migrations directory exists
        if !std::path::Path::new(&options.migrations_dir).exists() {
            return Ok(MigrationStatus::UpToDate);
        }

        // Use the internal verification
        match client.verify_migrations(&options.migrations_dir).await {
            Ok(true) => Ok(MigrationStatus::UpToDate),
            Ok(false) => {
                // Get pending migrations
                let pending = self.get_pending_migrations(options).await?;
                Ok(MigrationStatus::Pending {
                    count: pending.len(),
                    names: pending,
                })
            }
            Err(e) => Ok(MigrationStatus::Error(e.to_string())),
        }
    }

    async fn applied_migrations(&self) -> anyhow::Result<Vec<AppliedMigration>> {
        // Get from ClickHouse schema_migrations table
        let _client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        // Query the internal tracking table
        // This is handled by the migration tracker
        Ok(vec![])
    }

    async fn verify_checksums(&self, options: &MigrationOptions) -> anyhow::Result<bool> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client
            .verify_migrations(&options.migrations_dir)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

impl ClickHousePlugin {
    /// Get list of pending migration names
    async fn get_pending_migrations(
        &self,
        options: &MigrationOptions,
    ) -> anyhow::Result<Vec<String>> {
        let migrations_dir = std::path::Path::new(&options.migrations_dir);
        if !migrations_dir.exists() {
            return Ok(vec![]);
        }

        let mut pending = Vec::new();

        if let Ok(entries) = std::fs::read_dir(migrations_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "sql").unwrap_or(false)
                    && let Some(name) = path.file_stem().and_then(|n| n.to_str())
                {
                    pending.push(name.to_string());
                }
            }
        }

        pending.sort();
        Ok(pending)
    }
}

// =============================================================================
// Score Storage Support
// =============================================================================

#[async_trait]
impl ScoreStoragePlugin for ClickHousePlugin {
    async fn insert_scores(&self, scores: &[EvaluationScore]) -> anyhow::Result<()> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client.insert_scores(scores).await
    }

    async fn get_trace_scores(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client.get_trace_scores(project_id, trace_id).await
    }

    async fn get_session_scores(
        &self,
        project_id: uuid::Uuid,
        session_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client.get_session_scores(project_id, session_id).await
    }

    async fn get_trace_score_summary(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        let summary = client.get_trace_score_summary(project_id, trace_id).await?;
        Ok(summary.map(|s| serde_json::to_value(s).unwrap_or_default()))
    }

    async fn get_score_by_id(
        &self,
        project_id: uuid::Uuid,
        score_id: uuid::Uuid,
    ) -> anyhow::Result<Option<EvaluationScore>> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client.get_score_by_id(project_id, score_id).await
    }

    async fn soft_delete_score(
        &self,
        project_id: uuid::Uuid,
        score_id: uuid::Uuid,
    ) -> anyhow::Result<bool> {
        let client = self
            .client
            .get()
            .await
            .ok_or_else(|| anyhow::anyhow!("ClickHouse not initialized"))?;

        client.soft_delete_score(project_id, score_id).await
    }
}
