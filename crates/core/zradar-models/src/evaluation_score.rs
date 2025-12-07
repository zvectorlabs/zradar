//! Evaluation score data model

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Evaluation score for traces/spans
#[derive(Debug, Clone, Serialize, Deserialize, Row, sqlx::FromRow, PartialEq)]
pub struct EvaluationScore {
    // Identity
    pub id: String,
    pub tenant_id: String,
    pub project_id: String,

    // Timing (nanoseconds for Rust, converted to DateTime64(3) for ClickHouse)
    pub timestamp: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub event_ts: i64,

    // Entity Association
    pub trace_id: String,
    pub span_id: String, // Empty string if not applicable (links to spans.span_id)
    pub session_id: String,
    pub dataset_run_id: String,

    // Score Data
    pub name: String,
    pub value: f64,
    pub data_type: String, // Will be converted to/from EvalDataType
    pub string_value: String,

    // Evaluation Metadata
    pub source: String, // Will be converted to/from EvalSource
    pub comment: String,
    pub author_user_id: String,
    pub config_id: String,
    pub eval_execution_trace_id: String,
    pub queue_id: String,
    pub environment: String,

    // Additional Context
    pub service_name: String,
    pub agent_name: String,
    pub user_id: String,
    pub metadata: String, // JSON string

    // Event Sourcing
    pub is_deleted: i16,
}

/// Data type for evaluation scores
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvalDataType {
    Numeric,
    Categorical,
    Boolean,
}

impl From<EvalDataType> for String {
    fn from(dt: EvalDataType) -> String {
        match dt {
            EvalDataType::Numeric => "NUMERIC".to_string(),
            EvalDataType::Categorical => "CATEGORICAL".to_string(),
            EvalDataType::Boolean => "BOOLEAN".to_string(),
        }
    }
}

impl TryFrom<String> for EvalDataType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "NUMERIC" => Ok(EvalDataType::Numeric),
            "CATEGORICAL" => Ok(EvalDataType::Categorical),
            "BOOLEAN" => Ok(EvalDataType::Boolean),
            _ => Err(format!("Invalid data type: {}", s)),
        }
    }
}

impl TryFrom<&str> for EvalDataType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "NUMERIC" => Ok(EvalDataType::Numeric),
            "CATEGORICAL" => Ok(EvalDataType::Categorical),
            "BOOLEAN" => Ok(EvalDataType::Boolean),
            _ => Err(format!("Invalid data type: {}", s)),
        }
    }
}

/// Source of the evaluation
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvalSource {
    Annotation, // Manual human annotation
    Api,        // Direct API call
    Eval,       // Automated evaluation
}

impl From<EvalSource> for String {
    fn from(src: EvalSource) -> String {
        match src {
            EvalSource::Annotation => "ANNOTATION".to_string(),
            EvalSource::Api => "API".to_string(),
            EvalSource::Eval => "EVAL".to_string(),
        }
    }
}

impl TryFrom<String> for EvalSource {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "ANNOTATION" => Ok(EvalSource::Annotation),
            "API" => Ok(EvalSource::Api),
            "EVAL" => Ok(EvalSource::Eval),
            _ => Err(format!("Invalid source: {}", s)),
        }
    }
}

impl TryFrom<&str> for EvalSource {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "ANNOTATION" => Ok(EvalSource::Annotation),
            "API" => Ok(EvalSource::Api),
            "EVAL" => Ok(EvalSource::Eval),
            _ => Err(format!("Invalid source: {}", s)),
        }
    }
}

impl Default for EvaluationScore {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            tenant_id: String::new(),
            project_id: String::new(),
            timestamp: now,
            created_at: now,
            updated_at: now,
            event_ts: now,
            trace_id: String::new(),
            span_id: String::new(),
            session_id: String::new(),
            dataset_run_id: String::new(),
            name: String::new(),
            value: 0.0,
            data_type: "NUMERIC".to_string(),
            string_value: String::new(),
            source: "API".to_string(),
            comment: String::new(),
            author_user_id: String::new(),
            config_id: String::new(),
            eval_execution_trace_id: String::new(),
            queue_id: String::new(),
            environment: "default".to_string(),
            service_name: String::new(),
            agent_name: String::new(),
            user_id: String::new(),
            metadata: "{}".to_string(),
            is_deleted: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_data_type_conversion() {
        assert_eq!(String::from(EvalDataType::Numeric), "NUMERIC");
        assert_eq!(String::from(EvalDataType::Categorical), "CATEGORICAL");
        assert_eq!(String::from(EvalDataType::Boolean), "BOOLEAN");
    }

    #[test]
    fn test_eval_data_type_try_from_string() {
        assert_eq!(
            EvalDataType::try_from("NUMERIC".to_string()).unwrap(),
            EvalDataType::Numeric
        );
        assert_eq!(
            EvalDataType::try_from("CATEGORICAL".to_string()).unwrap(),
            EvalDataType::Categorical
        );
        assert_eq!(
            EvalDataType::try_from("BOOLEAN".to_string()).unwrap(),
            EvalDataType::Boolean
        );
        assert!(EvalDataType::try_from("INVALID".to_string()).is_err());
    }

    #[test]
    fn test_eval_data_type_try_from_str() {
        assert_eq!(
            EvalDataType::try_from("NUMERIC").unwrap(),
            EvalDataType::Numeric
        );
        assert_eq!(
            EvalDataType::try_from("CATEGORICAL").unwrap(),
            EvalDataType::Categorical
        );
        assert_eq!(
            EvalDataType::try_from("BOOLEAN").unwrap(),
            EvalDataType::Boolean
        );
        assert!(EvalDataType::try_from("INVALID").is_err());
    }

    #[test]
    fn test_eval_source_conversion() {
        assert_eq!(String::from(EvalSource::Annotation), "ANNOTATION");
        assert_eq!(String::from(EvalSource::Api), "API");
        assert_eq!(String::from(EvalSource::Eval), "EVAL");
    }

    #[test]
    fn test_eval_source_try_from_string() {
        assert_eq!(
            EvalSource::try_from("ANNOTATION".to_string()).unwrap(),
            EvalSource::Annotation
        );
        assert_eq!(
            EvalSource::try_from("API".to_string()).unwrap(),
            EvalSource::Api
        );
        assert_eq!(
            EvalSource::try_from("EVAL".to_string()).unwrap(),
            EvalSource::Eval
        );
        assert!(EvalSource::try_from("INVALID".to_string()).is_err());
    }

    #[test]
    fn test_eval_source_try_from_str() {
        assert_eq!(
            EvalSource::try_from("ANNOTATION").unwrap(),
            EvalSource::Annotation
        );
        assert_eq!(EvalSource::try_from("API").unwrap(), EvalSource::Api);
        assert_eq!(EvalSource::try_from("EVAL").unwrap(), EvalSource::Eval);
        assert!(EvalSource::try_from("INVALID").is_err());
    }

    #[test]
    fn test_evaluation_score_default() {
        let score = EvaluationScore::default();
        assert!(!score.id.is_empty());
        assert_eq!(score.data_type, "NUMERIC");
        assert_eq!(score.source, "API");
        assert_eq!(score.environment, "default");
        assert_eq!(score.is_deleted, 0i16);
        assert_eq!(score.metadata, "{}");
    }

    #[test]
    fn test_evaluation_score_serialization() {
        let score = EvaluationScore {
            id: "eval_123".to_string(),
            tenant_id: "tenant_1".to_string(),
            project_id: "proj_1".to_string(),
            trace_id: "trace_abc".to_string(),
            name: "accuracy".to_string(),
            value: 0.95,
            data_type: "NUMERIC".to_string(),
            source: "EVAL".to_string(),
            comment: "High accuracy".to_string(),
            ..Default::default()
        };

        // Test serialization
        let json = serde_json::to_string(&score).unwrap();
        assert!(json.contains("eval_123"));
        assert!(json.contains("accuracy"));
        assert!(json.contains("0.95"));

        // Test deserialization
        let deserialized: EvaluationScore = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "eval_123");
        assert_eq!(deserialized.name, "accuracy");
        assert_eq!(deserialized.value, 0.95);
    }

    #[test]
    fn test_evaluation_score_clone() {
        let score = EvaluationScore {
            id: "eval_123".to_string(),
            name: "hallucination".to_string(),
            value: 0.15,
            ..Default::default()
        };

        let cloned = score.clone();
        assert_eq!(score, cloned);
    }
}
