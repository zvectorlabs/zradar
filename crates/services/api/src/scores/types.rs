//! Score types and DTOs

// Re-export domain types and traits from core
pub use zradar_models::{EvalDataType, EvalSource};
pub use zradar_traits::{ScoreSummary, ScoreRepository};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Request to create a score
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateScoreRequest {
    pub trace_id: String,
    pub span_id: Option<String>,  // Links to spans.span_id
    pub session_id: Option<String>,
    pub dataset_run_id: Option<String>,
    pub name: String,
    pub value: f64,
    pub source: EvalSource,
    pub data_type: EvalDataType,
    pub string_value: Option<String>,
    pub comment: Option<String>,
    pub config_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// Score response
#[derive(Debug, Serialize, ToSchema)]
pub struct ScoreResponse {
    pub id: String,
    pub project_id: Uuid,
    pub trace_id: String,
    pub span_id: Option<String>,  // Links to spans.span_id
    pub session_id: Option<String>,
    pub dataset_run_id: Option<String>,
    pub name: String,
    pub value: f64,
    pub source: EvalSource,
    pub data_type: EvalDataType,
    pub string_value: Option<String>,
    pub comment: Option<String>,
    pub config_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Score summary response
#[derive(Debug, Serialize, ToSchema)]
pub struct ScoreSummaryResponse {
    pub name: String,
    pub avg_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub count: u64,
}

