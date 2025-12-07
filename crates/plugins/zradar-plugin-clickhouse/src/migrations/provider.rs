//! ClickHouse migration provider
//!
//! Implements MigrationProvider for ClickHouse plugin (stub for future use)

use sha2::{Digest, Sha256};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::ClickHouseClient;
use zradar_migrations::{MigrationInfo, MigrationProvider, MigrationResult, MigrationType};

#[allow(dead_code)]
pub struct ClickHouseMigrationProvider {
    _client: Arc<ClickHouseClient>,
}

impl ClickHouseMigrationProvider {
    #[allow(dead_code)]
    pub fn new(client: Arc<ClickHouseClient>) -> Self {
        Self { _client: client }
    }
}

impl MigrationProvider for ClickHouseMigrationProvider {
    fn plugin_name(&self) -> &str {
        "clickhouse"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn migration_type(&self) -> MigrationType {
        MigrationType::ClickHouse
    }

    fn migrations_dir(&self) -> Option<&str> {
        Some("./crates/plugins/zradar-plugin-clickhouse/migrations")
    }

    fn discover_migrations(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<MigrationInfo>>> + Send + '_>> {
        Box::pin(async move {
            // TODO: Implement ClickHouse migration discovery
            // For now, return empty as we're using Postgres only
            Ok(vec![])
        })
    }

    fn apply_migration<'a>(
        &'a self,
        _migration: &'a MigrationInfo,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<MigrationResult>> + Send + 'a>> {
        Box::pin(async move {
            // TODO: Implement ClickHouse migration application
            Ok(MigrationResult {
                success: true,
                duration_ms: 0,
                error: None,
            })
        })
    }

    fn verify_migration<'a>(
        &'a self,
        migration: &'a MigrationInfo,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            // Calculate current checksum
            let mut hasher = Sha256::new();
            hasher.update(migration.content.as_bytes());
            let actual_checksum = format!("{:x}", hasher.finalize());

            Ok(actual_checksum == migration.checksum)
        })
    }
}
