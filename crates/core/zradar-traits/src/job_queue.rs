//! Job queue trait definitions for telemetry ingestion
//!
//! Provides abstraction over different queue implementations:
//! - PostgreSQL-only (simple, up to 50 workers)
//! - Hybrid Redis+PostgreSQL (high-scale, 1000+ workers)

use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::RequestContext;

/// Generate hybrid sharded path from UUID
/// 
/// Combines shallow prefix sharding with full segment directories
/// 
/// UUID Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
/// 
/// Example: 1ec31d50-4669-4290-9bba-787b3c3e57f0
/// Returns:  1/e/c/3/1ec31d50/4669/4290/9bba/787b3c3e57f0/1ec31d50-4669-4290-9bba-787b3c3e57f0.pb
/// 
/// Structure (9 levels + filename):
/// - First 4 chars split: 1/e/c/3 (4 levels for fast distribution)
/// - First segment whole: 1ec31d50 (1 level for grouping)
/// - Remaining segments: 4669/4290/9bba/787b3c3e57f0 (4 levels for fine distribution)
/// - Filename: Full UUID with dashes
/// 
/// Benefits:
/// - Fast distribution via first 4 chars: 16^4 = 65,536 paths
/// - Further grouping by segment: 65,536 × 256 = 16.7M paths
/// - Perfect for 24-hour retention with up to 1M files
/// - Each leaf directory: ~1-10 files (optimal)
pub fn generate_sharded_path(job_id: &Uuid) -> String {
    let uuid_str = job_id.to_string();
    
    // Split UUID by dashes into 5 segments
    let parts: Vec<&str> = uuid_str.split('-').collect();
    
    // First 4 characters of first segment split individually
    let first_four_chars: Vec<String> = parts[0][..4]
        .chars()
        .map(|c| c.to_string())
        .collect();
    
    // Full segments
    let segment1 = parts[0];
    let segment2 = parts[1];
    let segment3 = parts[2];
    let segment4 = parts[3];
    let segment5 = parts[4];
    
    // Build path: 1/e/c/3/1ec31d50/4669/4290/9bba/787b3c3e57f0/uuid.pb
    format!(
        "{}/{}/{}/{}/{}/{}/{}.pb",
        first_four_chars.join("/"),
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        uuid_str
    )
}

/// Job status enum
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    Retrying,
}

/// Job type enum
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JobType {
    TraceIngestion,
    MetricIngestion,
}

/// Job model
#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub job_type: JobType,
    pub status: JobStatus,
    pub file_path: String,
    pub tenant_id: String,
    pub project_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub retry_count: i32,
    pub error_message: Option<String>,
    pub metadata: serde_json::Value,
}

/// Queue statistics for monitoring
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueStats {
    pub pending: u64,
    pub processing: u64,
    pub completed: u64,
    pub failed: u64,
    pub retrying: u64,
}

/// Abstract job queue interface
/// 
/// Implementations can use different backends:
/// - PostgreSQL only (simple, up to 50 workers)
/// - Redis + PostgreSQL (high-scale, 1000+ workers)
/// - In-memory (testing)
#[async_trait]
pub trait JobQueue: Send + Sync + 'static {
    /// Enqueue a new job
    /// 
    /// # Arguments
    /// * `data` - Raw OTLP protobuf data
    /// * `context` - Request context (tenant_id, project_id, etc.)
    /// 
    /// # Returns
    /// Job ID (UUID)
    async fn enqueue(
        &self,
        data: &[u8],
        context: &RequestContext,
    ) -> anyhow::Result<Uuid>;
    
    /// Dequeue next available job (blocking or timeout)
    /// 
    /// # Arguments
    /// * `worker_id` - Unique worker identifier
    /// 
    /// # Returns
    /// Job if available, None if queue empty
    async fn dequeue(&self, worker_id: &str) -> anyhow::Result<Option<Job>>;
    
    /// Mark job as successfully completed
    async fn complete(&self, job_id: Uuid) -> anyhow::Result<()>;
    
    /// Mark job as failed (may trigger retry)
    async fn fail(&self, job_id: Uuid, error: &str) -> anyhow::Result<()>;
    
    /// Get queue statistics (for monitoring)
    async fn get_stats(&self) -> anyhow::Result<QueueStats>;
    
    /// Health check
    async fn health_check(&self) -> anyhow::Result<bool>;
}

