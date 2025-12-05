//! PostgreSQL client management

use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::Arc;
use tokio::sync::RwLock;

/// PostgreSQL client wrapper
pub struct PostgresClient {
    pool: PgPool,
}

impl PostgresClient {
    /// Create from an existing pool (for when pool is already created externally)
    pub fn from_pool(pool: Arc<PgPool>) -> Self {
        Self { pool: (*pool).clone() }
    }
    
    /// Create from connection URL
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(url)
            .await?;
        
        tracing::info!("PostgreSQL connection pool established");
        
        Ok(Self { pool })
    }
    
    /// Create from configuration
    pub async fn from_config(config: &serde_json::Value) -> anyhow::Result<Self> {
        let url = config["url"].as_str()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL url required"))?;
        let max_connections = config["max_connections"].as_u64().unwrap_or(20) as u32;
        
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;
        
        tracing::info!(url = %url, max_connections = max_connections, "PostgreSQL connected");
        
        Ok(Self { pool })
    }
    
    /// Get the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
    
    /// Health check
    pub async fn health_check(&self) -> anyhow::Result<bool> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await?;
        Ok(true)
    }
}

/// Shared PostgreSQL client (thread-safe)
pub struct SharedPostgresClient {
    inner: RwLock<Option<Arc<PostgresClient>>>,
}

impl SharedPostgresClient {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }
    
    pub async fn initialize(&self, config: &serde_json::Value) -> anyhow::Result<()> {
        let client = PostgresClient::from_config(config).await?;
        *self.inner.write().await = Some(Arc::new(client));
        Ok(())
    }
    
    pub async fn get(&self) -> Option<Arc<PostgresClient>> {
        self.inner.read().await.clone()
    }
    
    pub async fn shutdown(&self) {
        *self.inner.write().await = None;
    }
}

impl Default for SharedPostgresClient {
    fn default() -> Self {
        Self::new()
    }
}

