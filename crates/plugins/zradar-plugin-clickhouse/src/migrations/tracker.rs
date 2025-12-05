//! Migration tracking in ClickHouse

use clickhouse::Client;
use clickhouse::Row;
use serde::Deserialize;
use crate::migrations::types::{AppliedMigration, MigrationResult};

pub struct MigrationTracker<'a> {
    client: &'a Client,
}

impl<'a> MigrationTracker<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }
    
    /// Ensure migration tracking table exists
    pub async fn ensure_table(&self) -> MigrationResult<()> {
        let create_table_sql = r#"
            CREATE TABLE IF NOT EXISTS _zradar_migrations (
                version String,
                description String,
                applied_at DateTime64(3) DEFAULT now64(3),
                checksum String,
                execution_time_ms UInt32
            ) ENGINE = MergeTree()
            ORDER BY (version, applied_at)
            SETTINGS index_granularity = 8192
        "#;
        
        self.client.query(create_table_sql).execute().await?;
        tracing::debug!("Migration tracking table initialized");
        Ok(())
    }
    
    /// Get list of applied migrations
    pub async fn get_applied(&self) -> MigrationResult<Vec<AppliedMigration>> {
        #[derive(Row, Deserialize)]
        struct AppliedRow {
            version: String,
            description: String,
            applied_at: String,
            checksum: String,
            execution_time_ms: u32,
        }
        
        let query = "SELECT version, description, toString(applied_at) as applied_at, checksum, execution_time_ms FROM _zradar_migrations ORDER BY version";
        
        let rows = self.client
            .query(query)
            .fetch_all::<AppliedRow>()
            .await?;
        
        let applied = rows.into_iter().map(|row| AppliedMigration {
            version: row.version,
            description: row.description,
            applied_at: row.applied_at,
            checksum: row.checksum,
            execution_time_ms: row.execution_time_ms,
        }).collect();
        
        Ok(applied)
    }
    
    /// Record a migration as applied
    pub async fn record(
        &self,
        version: &str,
        description: &str,
        checksum: &str,
        execution_time_ms: u32,
    ) -> MigrationResult<()> {
        let insert_query = format!(
            "INSERT INTO _zradar_migrations (version, description, checksum, execution_time_ms) VALUES ('{}', '{}', '{}', {})",
            version, description, checksum, execution_time_ms
        );
        
        self.client.query(&insert_query).execute().await?;
        Ok(())
    }
}

