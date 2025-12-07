//! PostgreSQL-only job queue implementation
//!
//! Best for:
//! - Small to medium scale (up to 50 workers)
//! - Simple deployment (no Redis needed)
//! - Complete durability (everything in PostgreSQL)
//!
//! Performance:
//! - Enqueue: ~10ms
//! - Dequeue: ~10ms
//! - Throughput: ~10K jobs/sec

use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use zradar_models::RequestContext;
use zradar_traits::{
    BlockStorage, Job, JobQueue, JobStatus, JobType, QueueStats, generate_sharded_path,
};

/// PostgreSQL-only job queue implementation
pub struct PostgresJobQueue {
    pg_pool: Arc<PgPool>,
    block_storage: Arc<dyn BlockStorage>,
}

impl PostgresJobQueue {
    /// Create new PostgreSQL job queue
    pub fn new(pg_pool: Arc<PgPool>, block_storage: Arc<dyn BlockStorage>) -> Self {
        Self {
            pg_pool,
            block_storage,
        }
    }

    /// Create from raw pool (convenience)
    pub fn from_pool(pg_pool: PgPool, block_storage: Arc<dyn BlockStorage>) -> Self {
        Self {
            pg_pool: Arc::new(pg_pool),
            block_storage,
        }
    }
}

#[async_trait]
impl JobQueue for PostgresJobQueue {
    async fn enqueue(&self, data: &[u8], context: &RequestContext) -> anyhow::Result<Uuid> {
        let job_id = Uuid::new_v4();

        // Use hybrid sharded path (9 levels)
        let key = generate_sharded_path(&job_id);

        // 1. Upload to block storage
        let file_path = self.block_storage.upload(&key, data).await?;

        // 2. Insert into PostgreSQL
        sqlx::query(
            r#"
            INSERT INTO ingestion_jobs 
            (id, job_type, status, file_path, tenant_id, project_id, created_at, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
            "#,
        )
        .bind(job_id)
        .bind("trace_ingestion")
        .bind("pending")
        .bind(&file_path)
        .bind(&context.tenant_id)
        .bind(&context.project_id)
        .bind(serde_json::json!({}))
        .execute(self.pg_pool.as_ref())
        .await?;

        tracing::info!(
            job_id = %job_id,
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            size = data.len(),
            "Job enqueued (PostgreSQL)"
        );

        Ok(job_id)
    }

    async fn dequeue(&self, worker_id: &str) -> anyhow::Result<Option<Job>> {
        // Use SKIP LOCKED for lock-free dequeue
        let result = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE ingestion_jobs
            SET status = 'processing',
                started_at = NOW(),
                metadata = metadata || jsonb_build_object('worker_id', $1::text)
            WHERE id = (
                SELECT id FROM ingestion_jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING 
                id, job_type, status, file_path, tenant_id, project_id,
                created_at, started_at, completed_at, retry_count, 
                error_message, metadata
            "#,
        )
        .bind(worker_id)
        .fetch_optional(self.pg_pool.as_ref())
        .await?;

        Ok(result.map(|row| {
            tracing::debug!(
                job_id = %row.id,
                worker_id = worker_id,
                "Job dequeued"
            );

            Job {
                id: row.id,
                job_type: JobType::TraceIngestion,
                status: JobStatus::Processing,
                file_path: row.file_path,
                tenant_id: row.tenant_id,
                project_id: row.project_id,
                created_at: row.created_at,
                started_at: row.started_at,
                completed_at: row.completed_at,
                retry_count: row.retry_count,
                error_message: row.error_message,
                metadata: row.metadata,
            }
        }))
    }

    async fn complete(&self, job_id: Uuid) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE ingestion_jobs 
            SET status = 'completed', 
                completed_at = NOW() 
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .execute(self.pg_pool.as_ref())
        .await?;

        tracing::info!(job_id = %job_id, "Job completed");

        Ok(())
    }

    async fn fail(&self, job_id: Uuid, error: &str) -> anyhow::Result<()> {
        // Get current retry count
        let record =
            sqlx::query_scalar::<_, i32>("SELECT retry_count FROM ingestion_jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(self.pg_pool.as_ref())
                .await?;

        const MAX_RETRIES: i32 = 3;
        let new_status = if record < MAX_RETRIES {
            "pending" // Retry
        } else {
            "failed" // Give up
        };

        sqlx::query(
            r#"
            UPDATE ingestion_jobs
            SET status = $2,
                error_message = $3,
                retry_count = retry_count + 1
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(new_status)
        .bind(error)
        .execute(self.pg_pool.as_ref())
        .await?;

        if new_status == "pending" {
            tracing::warn!(
                job_id = %job_id,
                retry_count = record + 1,
                error = error,
                "Job failed, will retry"
            );
        } else {
            tracing::error!(
                job_id = %job_id,
                error = error,
                "Job failed permanently"
            );
        }

        Ok(())
    }

    async fn get_stats(&self) -> anyhow::Result<QueueStats> {
        let stats = sqlx::query_as::<_, StatsRow>(
            r#"
            SELECT 
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'processing') as processing,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'retrying') as retrying
            FROM ingestion_jobs
            WHERE created_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .fetch_one(self.pg_pool.as_ref())
        .await?;

        Ok(QueueStats {
            pending: stats.pending.unwrap_or(0) as u64,
            processing: stats.processing.unwrap_or(0) as u64,
            completed: stats.completed.unwrap_or(0) as u64,
            failed: stats.failed.unwrap_or(0) as u64,
            retrying: stats.retrying.unwrap_or(0) as u64,
        })
    }

    async fn health_check(&self) -> anyhow::Result<bool> {
        sqlx::query("SELECT 1")
            .execute(self.pg_pool.as_ref())
            .await?;
        Ok(true)
    }
}

// Internal row types for sqlx
#[derive(sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    _job_type: String,
    _status: String,
    file_path: String,
    tenant_id: String,
    project_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    retry_count: i32,
    error_message: Option<String>,
    metadata: serde_json::Value,
}

#[derive(sqlx::FromRow)]
struct StatsRow {
    pending: Option<i64>,
    processing: Option<i64>,
    completed: Option<i64>,
    failed: Option<i64>,
    retrying: Option<i64>,
}
