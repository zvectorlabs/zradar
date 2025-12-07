//! Central migration registry
//!
//! Tracks all plugin migrations in a single PostgreSQL table

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use sqlx::PgPool;

use super::provider::MigrationProvider;
use super::types::*;

/// Central migration registry that tracks all plugin migrations in PostgreSQL
pub struct MigrationRegistry {
    pool: Arc<PgPool>,
    plugins: Arc<RwLock<HashMap<String, Box<dyn MigrationProvider>>>>,
}

impl MigrationRegistry {
    /// Create a new migration registry
    pub async fn new(pool: Arc<PgPool>) -> anyhow::Result<Self> {
        let registry = Self {
            pool,
            plugins: Arc::new(RwLock::new(HashMap::new())),
        };

        // ALWAYS create tracking table first on startup (idempotent)
        registry.create_tracking_table().await?;

        Ok(registry)
    }

    /// Create the _plugin_migrations tracking table (idempotent)
    /// This runs BEFORE any plugin migrations, solving the chicken-and-egg problem
    async fn create_tracking_table(&self) -> anyhow::Result<()> {
        info!("Ensuring migration tracking table exists...");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS _plugin_migrations (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                
                -- Plugin identification
                plugin_name VARCHAR(100) NOT NULL,
                plugin_version VARCHAR(50) NOT NULL,
                
                -- Migration details
                migration_version VARCHAR(100) NOT NULL,
                migration_name VARCHAR(255) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                
                -- Execution info
                applied_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                execution_time_ms INTEGER NOT NULL,
                
                -- Status tracking
                status VARCHAR(20) DEFAULT 'success' NOT NULL,
                error_message TEXT,
                
                -- Metadata
                migration_type VARCHAR(50) NOT NULL,
                metadata JSONB DEFAULT '{}'::jsonb,
                
                UNIQUE(plugin_name, migration_version)
            )
            "#,
        )
        .execute(&*self.pool)
        .await?;

        // Create indexes
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_plugin_migrations_plugin 
             ON _plugin_migrations(plugin_name, applied_at DESC)",
        )
        .execute(&*self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_plugin_migrations_status 
             ON _plugin_migrations(status)",
        )
        .execute(&*self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_plugin_migrations_type 
             ON _plugin_migrations(migration_type)",
        )
        .execute(&*self.pool)
        .await?;

        info!("✅ Migration tracking table ready");
        Ok(())
    }

    /// Register a plugin that provides migrations
    pub async fn register_plugin(&self, provider: Box<dyn MigrationProvider>) {
        let plugin_name = provider.plugin_name().to_string();
        let mut plugins = self.plugins.write().await;
        plugins.insert(plugin_name.clone(), provider);
        info!(plugin = %plugin_name, "Registered migration provider");
    }

    /// Get applied migrations for a plugin
    /// Note: Tracking table is always created on startup, so this is safe to call
    pub async fn get_applied_migrations(
        &self,
        plugin_name: &str,
    ) -> anyhow::Result<Vec<PluginMigration>> {
        let migrations = sqlx::query_as::<_, PluginMigration>(
            "SELECT * FROM _plugin_migrations 
             WHERE plugin_name = $1 AND status = 'success'
             ORDER BY applied_at ASC",
        )
        .bind(plugin_name)
        .fetch_all(&*self.pool)
        .await?;

        Ok(migrations)
    }

    /// Check if this is first run (no migrations applied yet)
    pub async fn is_first_run(&self) -> bool {
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _plugin_migrations")
            .fetch_one(&*self.pool)
            .await
            .unwrap_or(0);

        count == 0 // No migrations = first run
    }

    /// Run migrations for all registered plugins
    pub async fn run_all_migrations(&self, auto_migrate: bool) -> anyhow::Result<MigrationSummary> {
        let is_first_run = self.is_first_run().await;

        // On first run, always migrate regardless of config
        let _should_migrate = auto_migrate || is_first_run;

        if is_first_run {
            info!("🆕 First run detected - will auto-migrate all plugins");
        } else if !auto_migrate {
            info!("⏭️  Auto-migration disabled, skipping");
            return Ok(MigrationSummary::default());
        }

        let plugins = self.plugins.read().await;
        let mut summary = MigrationSummary::default();

        info!("🔄 Running migrations for {} plugins", plugins.len());

        for (name, provider) in plugins.iter() {
            let start = std::time::Instant::now();

            match self.run_plugin_migrations(provider.as_ref()).await {
                Ok(count) => {
                    let duration = start.elapsed().as_millis() as u64;
                    summary.successful += count;
                    summary.plugin_results.push(PluginMigrationResult {
                        plugin_name: name.clone(),
                        migrations_applied: count,
                        duration_ms: duration,
                        status: "success".to_string(),
                    });

                    if count > 0 {
                        info!(
                            plugin = name,
                            count = count,
                            duration_ms = duration,
                            "✅ Migrations applied"
                        );
                    } else {
                        info!(plugin = name, "✅ No pending migrations");
                    }
                }
                Err(e) => {
                    summary.failed += 1;
                    summary.errors.push(format!("{}: {}", name, e));
                    summary.plugin_results.push(PluginMigrationResult {
                        plugin_name: name.clone(),
                        migrations_applied: 0,
                        duration_ms: start.elapsed().as_millis() as u64,
                        status: "failed".to_string(),
                    });

                    error!(plugin = name, error = %e, "❌ Migration failed");

                    // FAIL FAST: Return error immediately
                    return Err(anyhow::anyhow!(
                        "Migration failed for plugin {}: {}",
                        name,
                        e
                    ));
                }
            }
        }

        Ok(summary)
    }

    /// Run migrations for a specific plugin
    async fn run_plugin_migrations(
        &self,
        provider: &dyn MigrationProvider,
    ) -> anyhow::Result<usize> {
        let plugin_name = provider.plugin_name();

        // Check if this is first run (tracking table doesn't exist)
        let is_first_run = self.is_first_run().await;

        // Discover all migrations
        let all_migrations = provider.discover_migrations().await?;

        let pending: Vec<_> = if is_first_run {
            // First run: apply ALL migrations without checking
            info!(
                plugin = plugin_name,
                "First run detected - applying all migrations"
            );
            all_migrations
        } else {
            // Not first run: check what's already applied
            let applied = self.get_applied_migrations(plugin_name).await?;
            let applied_versions: HashSet<String> = applied
                .iter()
                .map(|m| m.migration_version.clone())
                .collect();

            all_migrations
                .into_iter()
                .filter(|m| !applied_versions.contains(&m.version))
                .collect()
        };

        if pending.is_empty() {
            return Ok(0);
        }

        info!(
            plugin = plugin_name,
            count = pending.len(),
            "Found pending migrations"
        );

        // Apply each migration
        let mut applied_count = 0;
        for migration in pending {
            info!(
                plugin = plugin_name,
                migration = %migration.name,
                version = %migration.version,
                "Applying migration..."
            );

            let result = provider.apply_migration(&migration).await?;

            if !result.success {
                let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());

                // Try to record failure (will work after first migration creates the table)
                let _ = self
                    .record_migration(
                        plugin_name,
                        provider.plugin_version(),
                        &migration,
                        result.duration_ms,
                        provider.migration_type(),
                        false,
                        Some(&error_msg),
                    )
                    .await;

                anyhow::bail!("Migration {} failed: {}", migration.name, error_msg);
            }

            // Record successful migration (will work after first migration creates the table)
            let _ = self
                .record_migration(
                    plugin_name,
                    provider.plugin_version(),
                    &migration,
                    result.duration_ms,
                    provider.migration_type(),
                    true,
                    None,
                )
                .await;

            applied_count += 1;
        }

        Ok(applied_count)
    }

    /// Record a migration execution in the tracking table
    #[allow(clippy::too_many_arguments)]
    async fn record_migration(
        &self,
        plugin_name: &str,
        plugin_version: &str,
        migration: &MigrationInfo,
        duration_ms: u64,
        migration_type: MigrationType,
        success: bool,
        error: Option<&str>,
    ) -> anyhow::Result<()> {
        let status = if success { "success" } else { "failed" };

        sqlx::query(
            r#"
            INSERT INTO _plugin_migrations (
                plugin_name, plugin_version, migration_version, migration_name,
                checksum, execution_time_ms, status, error_message, migration_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (plugin_name, migration_version) DO NOTHING
            "#,
        )
        .bind(plugin_name)
        .bind(plugin_version)
        .bind(&migration.version)
        .bind(&migration.name)
        .bind(&migration.checksum)
        .bind(duration_ms as i32)
        .bind(status)
        .bind(error)
        .bind(migration_type.as_str())
        .execute(&*self.pool)
        .await?;

        Ok(())
    }

    /// Get migration status for all plugins
    pub async fn get_status(&self) -> anyhow::Result<HashMap<String, PluginMigrationStatus>> {
        let plugins = self.plugins.read().await;
        let mut statuses = HashMap::new();

        for (name, provider) in plugins.iter() {
            let applied = self.get_applied_migrations(name).await?;
            let all = provider.discover_migrations().await.unwrap_or_default();
            let pending = all.len().saturating_sub(applied.len());

            statuses.insert(
                name.clone(),
                PluginMigrationStatus {
                    plugin_name: name.clone(),
                    plugin_version: provider.plugin_version().to_string(),
                    applied_count: applied.len(),
                    pending_count: pending,
                    last_migration: applied.last().map(|m| m.migration_name.clone()),
                    last_applied_at: applied.last().map(|m| m.applied_at),
                },
            );
        }

        Ok(statuses)
    }
}
