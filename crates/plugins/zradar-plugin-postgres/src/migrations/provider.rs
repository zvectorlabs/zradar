//! PostgreSQL migration provider
//!
//! Implements MigrationProvider for PostgreSQL plugin

use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use sqlx::PgPool;
use sha2::{Sha256, Digest};

use zradar_migrations::{MigrationProvider, MigrationInfo, MigrationResult, MigrationType};

pub struct PostgresMigrationProvider {
    pool: Arc<PgPool>,
}

impl PostgresMigrationProvider {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

impl MigrationProvider for PostgresMigrationProvider {
    fn plugin_name(&self) -> &str {
        "postgres"
    }
    
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    
    fn migration_type(&self) -> MigrationType {
        MigrationType::Postgres
    }
    
    fn migrations_dir(&self) -> Option<&str> {
        Some("./crates/plugins/zradar-plugin-postgres/migrations")
    }
    
    fn discover_migrations(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<MigrationInfo>>> + Send + '_>> {
        Box::pin(async move {
        // Use sqlx's embedded migrations
        let migrator = &super::MIGRATIONS;
        
        let mut migrations = Vec::new();
        for migration in migrator.iter() {
            let content = migration.sql.to_string();
            
            // Calculate checksum from content
            let mut hasher = Sha256::new();
            hasher.update(content.as_bytes());
            let checksum = format!("{:x}", hasher.finalize());
            
            migrations.push(MigrationInfo {
                version: migration.version.to_string(),
                name: migration.description.to_string(),
                checksum,
                content,
            });
        }
        
        Ok(migrations)
        })
    }
    
    fn apply_migration<'a>(&'a self, migration: &'a MigrationInfo) -> Pin<Box<dyn Future<Output = anyhow::Result<MigrationResult>> + Send + 'a>> {
        Box::pin(async move {
        let start = std::time::Instant::now();
        
        // Use raw_sql to execute multiple statements in one migration file
        match sqlx::raw_sql(&migration.content)
            .execute(&*self.pool)
            .await
        {
            Ok(_) => Ok(MigrationResult {
                success: true,
                duration_ms: start.elapsed().as_millis() as u64,
                error: None,
            }),
            Err(e) => Ok(MigrationResult {
                success: false,
                duration_ms: start.elapsed().as_millis() as u64,
                error: Some(e.to_string()),
            }),
        }
        })
    }
    
    fn verify_migration<'a>(&'a self, migration: &'a MigrationInfo) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + 'a>> {
        Box::pin(async move {
        // Calculate current checksum
        let mut hasher = Sha256::new();
        hasher.update(migration.content.as_bytes());
        let actual_checksum = format!("{:x}", hasher.finalize());
        
        Ok(actual_checksum == migration.checksum)
        })
    }
}

