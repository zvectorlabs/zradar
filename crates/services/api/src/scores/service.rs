//! Scores service - evaluation score use case orchestration

use std::sync::Arc;
use uuid::Uuid;
use zradar_models::EvaluationScore;

use super::types::{CreateScoreRequest, ScoreRepository, ScoreResponse, ScoreSummaryResponse};
use crate::audit::{AuditEvent, AuditLogger, AuditStatus};
use crate::errors::{ControlError, Result};
use crate::projects::ProjectRepository;
use crate::rbac::PermissionChecker;

/// Scores service for evaluation score operations
pub struct ScoresService {
    pub repository: Arc<dyn ScoreRepository>,
    pub project_repository: Arc<dyn ProjectRepository>,
    pub rbac: Arc<dyn PermissionChecker>,
    pub audit: Arc<dyn AuditLogger>,
}

impl ScoresService {
    /// Create a new ScoresService
    pub fn new(
        repository: Arc<dyn ScoreRepository>,
        project_repository: Arc<dyn ProjectRepository>,
        rbac: Arc<dyn PermissionChecker>,
        audit: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            repository,
            project_repository,
            rbac,
            audit,
        }
    }

    /// Create a new evaluation score
    pub async fn create_score(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        request: CreateScoreRequest,
    ) -> Result<ScoreResponse> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:write",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        // Validate request
        if request.name.is_empty() {
            return Err(ControlError::InvalidInput(
                "Score name is required".to_string(),
            ));
        }
        if request.trace_id.is_empty() {
            return Err(ControlError::InvalidInput(
                "Trace ID is required".to_string(),
            ));
        }

        // Create score
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let score = EvaluationScore {
            id: format!("eval_{}", Uuid::new_v4()),
            tenant_id: tenant_id.clone(),
            project_id: project_id.to_string(),
            timestamp: now,
            created_at: now,
            updated_at: now,
            event_ts: now,
            trace_id: request.trace_id.clone(),
            span_id: request.span_id.clone().unwrap_or_default(),
            session_id: request.session_id.clone().unwrap_or_default(),
            dataset_run_id: request.dataset_run_id.clone().unwrap_or_default(),
            name: request.name.clone(),
            value: request.value,
            data_type: request.data_type.into(),
            string_value: request.string_value.clone().unwrap_or_default(),
            source: request.source.into(),
            comment: request.comment.clone().unwrap_or_default(),
            author_user_id: user_id.to_string(),
            config_id: request.config_id.clone().unwrap_or_default(),
            eval_execution_trace_id: String::new(),
            queue_id: String::new(),
            environment: "default".to_string(),
            service_name: String::new(),
            agent_name: String::new(),
            user_id: user_id.to_string(),
            metadata: serde_json::to_string(
                &request.metadata.clone().unwrap_or(serde_json::json!({})),
            )
            .unwrap_or_else(|_| "{}".to_string()),
            is_deleted: 0,
        };

        // Insert into storage (in test mode, this will automatically sync)
        self.repository
            .insert_scores(std::slice::from_ref(&score))
            .await?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: None,
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "score.created".to_string(),
                resource_type: Some("score".to_string()),
                resource_id: None,
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "score_id": score.id,
                    "trace_id": score.trace_id,
                    "name": score.name,
                    "value": score.value,
                })),
            })
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        Ok(ScoreResponse {
            id: score.id,
            project_id,
            trace_id: score.trace_id,
            span_id: if score.span_id.is_empty() {
                None
            } else {
                Some(score.span_id)
            },
            session_id: if score.session_id.is_empty() {
                None
            } else {
                Some(score.session_id)
            },
            dataset_run_id: if score.dataset_run_id.is_empty() {
                None
            } else {
                Some(score.dataset_run_id)
            },
            name: score.name,
            value: score.value,
            data_type: request.data_type,
            string_value: request.string_value,
            source: request.source,
            comment: request.comment,
            config_id: request.config_id,
            metadata: request.metadata,
            created_at: chrono::DateTime::from_timestamp_nanos(score.created_at),
        })
    }

    /// Get scores for a trace
    pub async fn get_trace_scores(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<Vec<ScoreResponse>> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:read",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        let scores = self
            .repository
            .get_trace_scores(&tenant_id, &project_id.to_string(), trace_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        Ok(scores
            .into_iter()
            .map(|s| self.score_to_response(s, project_id))
            .collect())
    }

    /// Get scores for a session
    pub async fn get_session_scores(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        session_id: &str,
    ) -> Result<Vec<ScoreResponse>> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:read",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        let scores = self
            .repository
            .get_session_scores(&tenant_id, &project_id.to_string(), session_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        Ok(scores
            .into_iter()
            .map(|s| self.score_to_response(s, project_id))
            .collect())
    }

    /// Get score summary for a trace
    pub async fn get_trace_score_summary(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        trace_id: &str,
    ) -> Result<Vec<ScoreSummaryResponse>> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:read",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        let summary = self
            .repository
            .get_trace_score_summary(&tenant_id, &project_id.to_string(), trace_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        Ok(summary
            .into_iter()
            .map(|s| ScoreSummaryResponse {
                name: s.name,
                avg_value: s.avg_value,
                min_value: s.min_value,
                max_value: s.max_value,
                count: s.count as u64,
            })
            .collect())
    }

    /// Get single score by ID
    pub async fn get_score_by_id(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        score_id: &str,
    ) -> Result<ScoreResponse> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:read",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        let score = self
            .repository
            .get_score_by_id(&tenant_id, &project_id.to_string(), score_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?
            .ok_or_else(|| ControlError::NotFound("Score not found".to_string()))?;

        Ok(self.score_to_response(score, project_id))
    }

    /// Delete a score
    pub async fn delete_score(
        &self,
        user_id: Uuid,
        tenant_id: String,
        project_id: Uuid,
        score_id: &str,
    ) -> Result<()> {
        // Look up project to get organization_id for RBAC
        let project = self
            .project_repository
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission with correct org_id
        self.rbac
            .check_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "scores:delete",
            )
            .await
            .map_err(|e| ControlError::Forbidden(e.to_string()))?;

        // Verify score exists and belongs to tenant/project
        let _score = self
            .repository
            .get_score_by_id(&tenant_id, &project_id.to_string(), score_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?
            .ok_or_else(|| ControlError::NotFound("Score not found".to_string()))?;

        // Delete score
        self.repository
            .delete_score(&tenant_id, &project_id.to_string(), score_id)
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: None,
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "score.deleted".to_string(),
                resource_type: Some("score".to_string()),
                resource_id: None,
                status: AuditStatus::Success,
                details: Some(serde_json::json!({"score_id": score_id})),
            })
            .await
            .map_err(|e| ControlError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Convert EvaluationScore to ScoreResponse
    fn score_to_response(&self, score: EvaluationScore, project_id: Uuid) -> ScoreResponse {
        ScoreResponse {
            id: score.id,
            project_id,
            trace_id: score.trace_id,
            span_id: if score.span_id.is_empty() {
                None
            } else {
                Some(score.span_id)
            },
            session_id: if score.session_id.is_empty() {
                None
            } else {
                Some(score.session_id)
            },
            dataset_run_id: if score.dataset_run_id.is_empty() {
                None
            } else {
                Some(score.dataset_run_id)
            },
            name: score.name,
            value: score.value,
            data_type: score
                .data_type
                .as_str()
                .try_into()
                .unwrap_or(zradar_models::EvalDataType::Numeric),
            string_value: if score.string_value.is_empty() {
                None
            } else {
                Some(score.string_value)
            },
            source: score
                .source
                .as_str()
                .try_into()
                .unwrap_or(zradar_models::EvalSource::Api),
            comment: if score.comment.is_empty() {
                None
            } else {
                Some(score.comment)
            },
            config_id: if score.config_id.is_empty() {
                None
            } else {
                Some(score.config_id)
            },
            metadata: serde_json::from_str(&score.metadata).ok(),
            created_at: chrono::DateTime::from_timestamp_nanos(score.created_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_to_response() {
        let score = EvaluationScore {
            id: "eval_123".to_string(),
            trace_id: "trace_abc".to_string(),
            name: "accuracy".to_string(),
            value: 0.95,
            data_type: "NUMERIC".to_string(),
            source: "API".to_string(),
            ..Default::default()
        };

        // Note: In a real test, we'd need to instantiate ScoresService with mocks
        // For now, this is just a structure test
        assert_eq!(score.name, "accuracy");
        assert_eq!(score.value, 0.95);
    }
}
