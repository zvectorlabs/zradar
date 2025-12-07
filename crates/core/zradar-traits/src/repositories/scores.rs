//! Score repository trait

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use zradar_models::EvaluationScore;

/// Score summary for aggregations
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScoreSummary {
    pub name: String,
    pub avg_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub count: i32, // i32 for PostgreSQL compatibility (COUNT returns INTEGER)
}

/// Repository trait for score persistence
#[async_trait]
pub trait ScoreRepository: Send + Sync {
    /// Insert evaluation scores
    async fn insert_scores(&self, scores: &[EvaluationScore]) -> anyhow::Result<()>;

    /// Get scores for a trace
    async fn get_trace_scores(
        &self,
        tenant_id: &str,
        project_id: &str,
        trace_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>>;

    /// Get scores for a session
    async fn get_session_scores(
        &self,
        tenant_id: &str,
        project_id: &str,
        session_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>>;

    /// Get score summary for a trace
    async fn get_trace_score_summary(
        &self,
        tenant_id: &str,
        project_id: &str,
        trace_id: &str,
    ) -> anyhow::Result<Vec<ScoreSummary>>;

    /// Get score by ID
    async fn get_score_by_id(
        &self,
        tenant_id: &str,
        project_id: &str,
        score_id: &str,
    ) -> anyhow::Result<Option<EvaluationScore>>;

    /// Delete a score
    async fn delete_score(
        &self,
        tenant_id: &str,
        project_id: &str,
        score_id: &str,
    ) -> anyhow::Result<()>;
}
