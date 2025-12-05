//! PostgreSQL scores repository implementation

use async_trait::async_trait;
use std::sync::Arc;
use sqlx;

use zradar_traits::{ScoreRepository, ScoreSummary};
use zradar_models::EvaluationScore;
use crate::client::PostgresClient;

pub struct PostgresScoreRepository {
    client: Arc<PostgresClient>,
}

impl PostgresScoreRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ScoreRepository for PostgresScoreRepository {
    async fn insert_scores(&self, scores: &[EvaluationScore]) -> anyhow::Result<()> {
        if scores.is_empty() {
            return Ok(());
        }
        
        for score in scores {
            sqlx::query(
                r#"
                INSERT INTO evaluation_scores (
                    id, tenant_id, project_id, timestamp, created_at, updated_at, event_ts,
                    trace_id, span_id, session_id, dataset_run_id, name, value,
                    data_type, string_value, source, comment, author_user_id, config_id,
                    eval_execution_trace_id, queue_id, environment, service_name, agent_name,
                    user_id, metadata, is_deleted
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
                )
                ON CONFLICT (id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    value = EXCLUDED.value,
                    metadata = EXCLUDED.metadata
                "#,
            )
            .bind(&score.id)
            .bind(&score.tenant_id)
            .bind(&score.project_id)
            .bind(score.timestamp)
            .bind(score.created_at)
            .bind(score.updated_at)
            .bind(score.event_ts)
            .bind(&score.trace_id)
            .bind(&score.span_id)
            .bind(&score.session_id)
            .bind(&score.dataset_run_id)
            .bind(&score.name)
            .bind(score.value)
            .bind(&score.data_type)
            .bind(&score.string_value)
            .bind(&score.source)
            .bind(&score.comment)
            .bind(&score.author_user_id)
            .bind(&score.config_id)
            .bind(&score.eval_execution_trace_id)
            .bind(&score.queue_id)
            .bind(&score.environment)
            .bind(&score.service_name)
            .bind(&score.agent_name)
            .bind(&score.user_id)
            .bind(&score.metadata)
            .bind(score.is_deleted)
            .execute(self.client.pool())
            .await?;
        }
        
        Ok(())
    }
    
    async fn get_trace_scores(&self, tenant_id: &str, project_id: &str, trace_id: &str) -> anyhow::Result<Vec<EvaluationScore>> {
        let scores = sqlx::query_as::<_, EvaluationScore>(
            r#"
            SELECT * FROM evaluation_scores
            WHERE tenant_id = $1 AND project_id = $2 AND trace_id = $3 AND is_deleted = 0
            ORDER BY created_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(trace_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(scores)
    }
    
    async fn get_session_scores(&self, tenant_id: &str, project_id: &str, session_id: &str) -> anyhow::Result<Vec<EvaluationScore>> {
        let scores = sqlx::query_as::<_, EvaluationScore>(
            r#"
            SELECT * FROM evaluation_scores
            WHERE tenant_id = $1 AND project_id = $2 AND session_id = $3 AND is_deleted = 0
            ORDER BY created_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(session_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(scores)
    }
    
    async fn get_trace_score_summary(&self, tenant_id: &str, project_id: &str, trace_id: &str) -> anyhow::Result<Vec<ScoreSummary>> {
        let summary = sqlx::query_as::<_, ScoreSummary>(
            r#"
            SELECT
                name,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                COUNT(*)::INTEGER as count
            FROM evaluation_scores
            WHERE tenant_id = $1 AND project_id = $2 AND trace_id = $3 AND is_deleted = 0
            GROUP BY name
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(trace_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(summary)
    }
    
    async fn get_score_by_id(&self, tenant_id: &str, project_id: &str, score_id: &str) -> anyhow::Result<Option<EvaluationScore>> {
        let score = sqlx::query_as::<_, EvaluationScore>(
            r#"
            SELECT * FROM evaluation_scores
            WHERE tenant_id = $1 AND project_id = $2 AND id = $3 AND is_deleted = 0
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(score_id)
        .fetch_optional(self.client.pool())
        .await?;
        
        Ok(score)
    }
    
    async fn delete_score(&self, tenant_id: &str, project_id: &str, score_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE evaluation_scores
            SET is_deleted = 1, updated_at = $4
            WHERE tenant_id = $1 AND project_id = $2 AND id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(score_id)
        .bind(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
        .execute(self.client.pool())
        .await?;
        
        Ok(())
    }
}
