//! ClickHouse client - connection management

use clickhouse::Client;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use zradar_models::EvaluationScore;

/// ClickHouse client wrapper
pub struct ClickHouseClient {
    client: Client,
    test_mode: bool,
}

impl ClickHouseClient {
    /// Create a new ClickHouse client from configuration
    pub async fn from_config(config: &serde_json::Value) -> Result<Self> {
        let url = config["url"].as_str()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse url required"))?;
        let user = config["user"].as_str().unwrap_or("default");
        let password = config["password"].as_str().unwrap_or("");
        let database = config["database"].as_str().unwrap_or("default");
        
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(database)
            .with_compression(clickhouse::Compression::Lz4);
        
        // Test connection
        let result: Result<u8, clickhouse::error::Error> = client
            .query("SELECT 1")
            .fetch_one()
            .await;
        
        match result {
            Ok(_) => {
                tracing::info!(
                    url = %url,
                    database = %database,
                    "ClickHouse connection established"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    url = %url,
                    "ClickHouse connection failed"
                );
                return Err(e.into());
            }
        }
        
        // Detect test mode
        let test_mode = std::env::var("ZRADAR_TEST_MODE").is_ok() 
            || database.contains("test")
            || config["test_mode"].as_bool().unwrap_or(false);
        
        if test_mode {
            tracing::info!("ClickHouse plugin running in TEST MODE");
        }
        
        Ok(Self { client, test_mode })
    }
    
    /// Get the underlying client reference
    pub fn client(&self) -> &Client {
        &self.client
    }
    
    /// Clone the underlying client as Arc
    pub fn client_arc(&self) -> Arc<Client> {
        Arc::new(self.client.clone())
    }
    
    /// Check if in test mode
    pub fn is_test_mode(&self) -> bool {
        self.test_mode
    }
    
    /// Health check
    pub async fn health_check(&self) -> Result<bool> {
        let result: Result<u8, _> = self.client.query("SELECT 1").fetch_one().await;
        Ok(result.is_ok())
    }
    
    /// Stub for legacy migration support
    pub async fn run_migrations<P: AsRef<std::path::Path>>(&self, _migrations_dir: P) -> anyhow::Result<()> {
        // TODO: Implement using MigrationRegistry
        Ok(())
    }
    
    /// Stub for legacy migration verification
    pub async fn verify_migrations<P: AsRef<std::path::Path>>(&self, _migrations_dir: P) -> anyhow::Result<bool> {
        // TODO: Implement using MigrationRegistry
        Ok(true)
    }
    
    // =========================================================================
    // Score Operations
    // =========================================================================
    
    /// Insert evaluation scores
    pub async fn insert_scores(&self, scores: &[EvaluationScore]) -> Result<()> {
        if scores.is_empty() {
            return Ok(());
        }
        
        let mut insert = self.client.insert("evaluation_scores")?;
        for score in scores {
            insert.write(score).await?;
        }
        insert.end().await?;
        
        tracing::info!(count = scores.len(), "Inserted evaluation scores");
        
        // Test mode: force synchronous visibility
        if self.test_mode {
            self.client.query("OPTIMIZE TABLE evaluation_scores FINAL SETTINGS mutations_sync=2")
                .execute().await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        Ok(())
    }
    
    /// Get scores for a trace
    pub async fn get_trace_scores(
        &self,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<Vec<EvaluationScore>> {
        let query = format!(
            "SELECT * FROM evaluation_scores \
             WHERE project_id = '{}' AND trace_id = '{}' \
             AND is_deleted = 0 ORDER BY timestamp DESC",
            project_id, trace_id
        );
        
        let scores = self.client.query(&query)
            .fetch_all::<EvaluationScore>()
            .await?;
        
        Ok(scores)
    }
    
    /// Get scores for a session
    pub async fn get_session_scores(
        &self,
        project_id: Uuid,
        session_id: &str,
    ) -> Result<Vec<EvaluationScore>> {
        let query = format!(
            "SELECT * FROM evaluation_scores \
             WHERE project_id = '{}' AND session_id = '{}' \
             AND is_deleted = 0 ORDER BY timestamp DESC",
            project_id, session_id
        );
        
        let scores = self.client.query(&query)
            .fetch_all::<EvaluationScore>()
            .await?;
        
        Ok(scores)
    }
    
    /// Get score summary for a trace (returns JSON-like structure)
    pub async fn get_trace_score_summary(
        &self,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<Option<ScoreSummary>> {
        let query = format!(
            "SELECT \
                name, \
                avg(value) as avg_value, \
                min(value) as min_value, \
                max(value) as max_value, \
                count(*) as count \
             FROM evaluation_scores \
             WHERE project_id = '{}' AND trace_id = '{}' AND is_deleted = 0 \
             GROUP BY name",
            project_id, trace_id
        );
        
        let results: Vec<ScoreSummary> = self.client.query(&query)
            .fetch_all()
            .await?;
        
        Ok(results.into_iter().next())
    }
    
    /// Get a specific score by ID
    pub async fn get_score_by_id(
        &self,
        project_id: Uuid,
        score_id: Uuid,
    ) -> Result<Option<EvaluationScore>> {
        let query = format!(
            "SELECT * FROM evaluation_scores \
             WHERE project_id = '{}' AND id = '{}' AND is_deleted = 0",
            project_id, score_id
        );
        
        let scores: Vec<EvaluationScore> = self.client.query(&query)
            .fetch_all()
            .await?;
        
        Ok(scores.into_iter().next())
    }
    
    /// Soft delete a score
    pub async fn soft_delete_score(
        &self,
        project_id: Uuid,
        score_id: Uuid,
    ) -> Result<bool> {
        let query = format!(
            "ALTER TABLE evaluation_scores UPDATE is_deleted = 1 \
             WHERE project_id = '{}' AND id = '{}'",
            project_id, score_id
        );
        
        self.client.query(&query).execute().await?;
        
        Ok(true)
    }
}

/// Score summary for aggregation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, clickhouse::Row)]
pub struct ScoreSummary {
    pub name: String,
    pub avg_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub count: i32,
}

/// Shared ClickHouse client (thread-safe)
pub struct SharedClickHouseClient {
    inner: RwLock<Option<Arc<ClickHouseClient>>>,
}

impl SharedClickHouseClient {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }
    
    pub async fn initialize(&self, config: &serde_json::Value) -> Result<()> {
        let client = ClickHouseClient::from_config(config).await?;
        *self.inner.write().await = Some(Arc::new(client));
        Ok(())
    }
    
    pub async fn get(&self) -> Option<Arc<ClickHouseClient>> {
        self.inner.read().await.clone()
    }
    
    pub async fn shutdown(&self) {
        *self.inner.write().await = None;
    }
}

impl Default for SharedClickHouseClient {
    fn default() -> Self {
        Self::new()
    }
}

