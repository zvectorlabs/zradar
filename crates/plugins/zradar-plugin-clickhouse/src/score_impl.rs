//! ScoreRepository trait implementation for ClickHouseClient

use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::EvaluationScore;
use zradar_traits::{ScoreRepository, ScoreSummary};

use crate::ClickHouseClient;

#[async_trait]
impl ScoreRepository for ClickHouseClient {
    async fn insert_scores(&self, scores: &[EvaluationScore]) -> anyhow::Result<()> {
        self.insert_scores(scores).await
    }

    async fn get_trace_scores(
        &self,
        _tenant_id: &str,
        project_id: &str,
        trace_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>> {
        let project_uuid = Uuid::parse_str(project_id)?;
        self.get_trace_scores(project_uuid, trace_id).await
    }

    async fn get_session_scores(
        &self,
        _tenant_id: &str,
        project_id: &str,
        session_id: &str,
    ) -> anyhow::Result<Vec<EvaluationScore>> {
        let project_uuid = Uuid::parse_str(project_id)?;
        self.get_session_scores(project_uuid, session_id).await
    }

    async fn get_trace_score_summary(
        &self,
        _tenant_id: &str,
        project_id: &str,
        trace_id: &str,
    ) -> anyhow::Result<Vec<ScoreSummary>> {
        let project_uuid = Uuid::parse_str(project_id)?;
        let summary = self.get_trace_score_summary(project_uuid, trace_id).await?;

        // Convert ClickHouse ScoreSummary to traits ScoreSummary
        Ok(summary
            .into_iter()
            .map(|s| ScoreSummary {
                name: s.name,
                avg_value: s.avg_value,
                min_value: s.min_value,
                max_value: s.max_value,
                count: s.count,
            })
            .collect())
    }

    async fn get_score_by_id(
        &self,
        _tenant_id: &str,
        project_id: &str,
        score_id: &str,
    ) -> anyhow::Result<Option<EvaluationScore>> {
        let project_uuid = Uuid::parse_str(project_id)?;
        let score_uuid = Uuid::parse_str(score_id)?;
        self.get_score_by_id(project_uuid, score_uuid).await
    }

    async fn delete_score(
        &self,
        _tenant_id: &str,
        project_id: &str,
        score_id: &str,
    ) -> anyhow::Result<()> {
        let project_uuid = Uuid::parse_str(project_id)?;
        let score_uuid = Uuid::parse_str(score_id)?;
        self.soft_delete_score(project_uuid, score_uuid).await?;
        Ok(())
    }
}
