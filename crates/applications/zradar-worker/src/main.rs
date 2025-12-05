//! zradar-worker - Asynchronous job processing for telemetry ingestion
//! 
//! This binary processes jobs from the queue and inserts them into PostgreSQL.
//! It can be scaled independently from the ingestion tier.
//!
//! Plugin-based architecture for flexible telemetry storage backends.

mod worker;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use zradar_models::Config;
use zradar_traits::TelemetryWriter;

// Plugins
use zradar_plugin_postgres::{PostgresClient, PostgresJobQueue, PostgresTelemetryRepository};
use zradar_plugin_local::LocalBlockStorage;

// Local worker
use worker::WorkerPool;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,zradar=debug"))
        )
        .init();

    info!("🚀 Starting zradar-worker (processing tier)...");

    // Load configuration
    let config = Config::load()?;
    info!("✅ Configuration loaded");

    // Connect to PostgreSQL
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    
    let pg_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;
    
    let pg_pool = Arc::new(pg_pool);
    info!("✅ Connected to PostgreSQL");

    // Note: Worker doesn't run migrations - server handles that
    // Workers just connect to an already-migrated database
    info!("⏭️  Skipping migrations (handled by server)");

    // Initialize block storage (from plugin)
    let storage_path = std::env::var("STORAGE_PATH")
        .unwrap_or_else(|_| "./data/trace-batches".to_string());
    
    let block_storage = Arc::new(LocalBlockStorage::new(&storage_path));
    info!("✅ Block storage initialized: local ({})", storage_path);

    // Initialize job queue (from plugin)
    let job_queue = Arc::new(PostgresJobQueue::new(pg_pool.clone(), block_storage.clone()));
    info!("✅ Job queue initialized: postgres");

    // Create PostgreSQL client for telemetry writer
    let postgres_config = serde_json::json!({
        "url": database_url,
    });
    
    let postgres_client = Arc::new(
        PostgresClient::from_config(&postgres_config).await?
    );
    
    // Create telemetry writer (uses TelemetryWriter trait for flexibility)
    let writer: Arc<dyn TelemetryWriter> = Arc::new(PostgresTelemetryRepository::new(postgres_client));
    info!("✅ Telemetry writer initialized (PostgreSQL)");

    // Start worker pool
    let num_workers = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    
    let worker_pool = WorkerPool::new(
        num_workers,
        job_queue,
        block_storage,
        writer,
    );
    
    worker_pool.start().await;
    
    info!("✅ Worker pool started with {} workers", num_workers);
    info!("💾 Cleanup via BlockStorage: Local=immediate, S3=lifecycle policy (24h)");
    info!("🎯 Ready to process jobs!");

    // Keep running until Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Shutting down gracefully...");

    Ok(())
}
