//! Arrow schema and RecordBatch ↔ EvaluationScore conversions.
//!
//! Mirrors `zradar_models::EvaluationScore` field-for-field. The scores
//! signal is persisted to Parquet alongside traces/metrics/logs under
//! `file_list.signal_type = 'scores'` per Phase 1 R1.8 / OQ9.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{ArrayRef, Float64Array, Int16Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zradar_models::EvaluationScore;

/// Return the Arrow schema for the EvaluationScore type.
pub fn score_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        // Timing (nanoseconds)
        Field::new("timestamp", DataType::Int64, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("event_ts", DataType::Int64, false),
        // Entity association
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("dataset_run_id", DataType::Utf8, false),
        // Score data
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("string_value", DataType::Utf8, false),
        // Evaluation metadata
        Field::new("source", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, false),
        Field::new("author_user_id", DataType::Utf8, false),
        Field::new("config_id", DataType::Utf8, false),
        Field::new("eval_execution_trace_id", DataType::Utf8, false),
        Field::new("queue_id", DataType::Utf8, false),
        Field::new("environment", DataType::Utf8, false),
        // Additional context
        Field::new("service_name", DataType::Utf8, false),
        Field::new("agent_name", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, false),
        // Event sourcing
        Field::new("is_deleted", DataType::Int16, false),
    ])
}

/// Convert a slice of `EvaluationScore` rows into an Arrow `RecordBatch`.
pub fn scores_to_record_batch(scores: &[EvaluationScore]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(score_arrow_schema());

    let id = StringArray::from_iter_values(scores.iter().map(|s| s.id.as_str()));
    let workspace_id =
        StringArray::from_iter_values(scores.iter().map(|s| s.workspace_id.as_str()));
    let timestamp: Int64Array = scores.iter().map(|s| s.timestamp).collect();
    let created_at: Int64Array = scores.iter().map(|s| s.created_at).collect();
    let updated_at: Int64Array = scores.iter().map(|s| s.updated_at).collect();
    let event_ts: Int64Array = scores.iter().map(|s| s.event_ts).collect();
    let trace_id = StringArray::from_iter_values(scores.iter().map(|s| s.trace_id.as_str()));
    let span_id = StringArray::from_iter_values(scores.iter().map(|s| s.span_id.as_str()));
    let session_id = StringArray::from_iter_values(scores.iter().map(|s| s.session_id.as_str()));
    let dataset_run_id =
        StringArray::from_iter_values(scores.iter().map(|s| s.dataset_run_id.as_str()));
    let name = StringArray::from_iter_values(scores.iter().map(|s| s.name.as_str()));
    let value: Float64Array = scores.iter().map(|s| s.value).collect();
    let data_type = StringArray::from_iter_values(scores.iter().map(|s| s.data_type.as_str()));
    let string_value =
        StringArray::from_iter_values(scores.iter().map(|s| s.string_value.as_str()));
    let source = StringArray::from_iter_values(scores.iter().map(|s| s.source.as_str()));
    let comment = StringArray::from_iter_values(scores.iter().map(|s| s.comment.as_str()));
    let author_user_id =
        StringArray::from_iter_values(scores.iter().map(|s| s.author_user_id.as_str()));
    let config_id = StringArray::from_iter_values(scores.iter().map(|s| s.config_id.as_str()));
    let eval_execution_trace_id =
        StringArray::from_iter_values(scores.iter().map(|s| s.eval_execution_trace_id.as_str()));
    let queue_id = StringArray::from_iter_values(scores.iter().map(|s| s.queue_id.as_str()));
    let environment = StringArray::from_iter_values(scores.iter().map(|s| s.environment.as_str()));
    let service_name =
        StringArray::from_iter_values(scores.iter().map(|s| s.service_name.as_str()));
    let agent_name = StringArray::from_iter_values(scores.iter().map(|s| s.agent_name.as_str()));
    let user_id = StringArray::from_iter_values(scores.iter().map(|s| s.user_id.as_str()));
    let metadata = StringArray::from_iter_values(scores.iter().map(|s| s.metadata.as_str()));
    let is_deleted: Int16Array = scores.iter().map(|s| s.is_deleted).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id),
        Arc::new(workspace_id),
        Arc::new(timestamp),
        Arc::new(created_at),
        Arc::new(updated_at),
        Arc::new(event_ts),
        Arc::new(trace_id),
        Arc::new(span_id),
        Arc::new(session_id),
        Arc::new(dataset_run_id),
        Arc::new(name),
        Arc::new(value),
        Arc::new(data_type),
        Arc::new(string_value),
        Arc::new(source),
        Arc::new(comment),
        Arc::new(author_user_id),
        Arc::new(config_id),
        Arc::new(eval_execution_trace_id),
        Arc::new(queue_id),
        Arc::new(environment),
        Arc::new(service_name),
        Arc::new(agent_name),
        Arc::new(user_id),
        Arc::new(metadata),
        Arc::new(is_deleted),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to construct EvaluationScore RecordBatch")
}

/// Convert an Arrow `RecordBatch` back into a `Vec<EvaluationScore>`.
pub fn record_batch_to_scores(batch: &RecordBatch) -> anyhow::Result<Vec<EvaluationScore>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(vec![]);
    }

    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("column {} is not StringArray", $name))?
        };
    }
    macro_rules! i64_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("column {} is not Int64Array", $name))?
        };
    }
    macro_rules! i16_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| anyhow!("column {} is not Int16Array", $name))?
        };
    }
    macro_rules! f64_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("column {} is not Float64Array", $name))?
        };
    }

    let id = str_col!("id");
    let workspace_id_col = str_col!("workspace_id");
    let timestamp = i64_col!("timestamp");
    let created_at = i64_col!("created_at");
    let updated_at = i64_col!("updated_at");
    let event_ts = i64_col!("event_ts");
    let trace_id = str_col!("trace_id");
    let span_id = str_col!("span_id");
    let session_id = str_col!("session_id");
    let dataset_run_id = str_col!("dataset_run_id");
    let name = str_col!("name");
    let value = f64_col!("value");
    let data_type = str_col!("data_type");
    let string_value = str_col!("string_value");
    let source = str_col!("source");
    let comment = str_col!("comment");
    let author_user_id = str_col!("author_user_id");
    let config_id = str_col!("config_id");
    let eval_execution_trace_id = str_col!("eval_execution_trace_id");
    let queue_id = str_col!("queue_id");
    let environment = str_col!("environment");
    let service_name = str_col!("service_name");
    let agent_name = str_col!("agent_name");
    let user_id = str_col!("user_id");
    let metadata = str_col!("metadata");
    let is_deleted = i16_col!("is_deleted");

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(EvaluationScore {
            id: id.value(i).to_string(),
            workspace_id: workspace_id_col.value(i).to_string(),
            timestamp: timestamp.value(i),
            created_at: created_at.value(i),
            updated_at: updated_at.value(i),
            event_ts: event_ts.value(i),
            trace_id: trace_id.value(i).to_string(),
            span_id: span_id.value(i).to_string(),
            session_id: session_id.value(i).to_string(),
            dataset_run_id: dataset_run_id.value(i).to_string(),
            name: name.value(i).to_string(),
            value: value.value(i),
            data_type: data_type.value(i).to_string(),
            string_value: string_value.value(i).to_string(),
            source: source.value(i).to_string(),
            comment: comment.value(i).to_string(),
            author_user_id: author_user_id.value(i).to_string(),
            config_id: config_id.value(i).to_string(),
            eval_execution_trace_id: eval_execution_trace_id.value(i).to_string(),
            queue_id: queue_id.value(i).to_string(),
            environment: environment.value(i).to_string(),
            service_name: service_name.value(i).to_string(),
            agent_name: agent_name.value(i).to_string(),
            user_id: user_id.value(i).to_string(),
            metadata: metadata.value(i).to_string(),
            is_deleted: is_deleted.value(i),
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_score(trace: &str, project: &str, name: &str, value: f64) -> EvaluationScore {
        EvaluationScore {
            trace_id: trace.to_string(),
            workspace_id: project.to_string(),
            name: name.to_string(),
            value,
            ..EvaluationScore::default()
        }
    }

    #[test]
    fn test_score_arrow_schema_field_count() {
        let schema = score_arrow_schema();
        // 26 fields total (matches the EvaluationScore struct)
        assert_eq!(schema.fields().len(), 26);
    }

    #[test]
    fn test_scores_to_record_batch_empty() {
        let batch = scores_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 26);
    }

    #[test]
    fn test_round_trip_single_score() {
        let s = make_score("trace-001", "proj-001", "faithfulness", 0.93);
        let batch = scores_to_record_batch(std::slice::from_ref(&s)).unwrap();
        let recovered = record_batch_to_scores(&batch).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].trace_id, "trace-001");
        assert_eq!(recovered[0].workspace_id, "proj-001");
        assert_eq!(recovered[0].name, "faithfulness");
        assert!((recovered[0].value - 0.93).abs() < f64::EPSILON);
        // Defaults preserved through round-trip
        assert_eq!(recovered[0].data_type, "NUMERIC");
        assert_eq!(recovered[0].source, "API");
        assert_eq!(recovered[0].is_deleted, 0);
    }

    #[test]
    fn test_round_trip_multiple_scores() {
        let scores = vec![
            make_score("t1", "p1", "accuracy", 0.95),
            make_score("t2", "p1", "hallucination", 0.10),
            make_score("t3", "p1", "toxicity", 0.02),
        ];
        let batch = scores_to_record_batch(&scores).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let recovered = record_batch_to_scores(&batch).unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0].name, "accuracy");
        assert_eq!(recovered[1].name, "hallucination");
        assert_eq!(recovered[2].name, "toxicity");
    }

    #[test]
    fn test_round_trip_preserves_all_fields() {
        let s = EvaluationScore {
            id: "eval_123".to_string(),
            workspace_id: "proj_x".to_string(),
            timestamp: 1_700_000_000_000_000_000,
            created_at: 1_700_000_000_000_000_001,
            updated_at: 1_700_000_000_000_000_002,
            event_ts: 1_700_000_000_000_000_003,
            trace_id: "trace_abc".to_string(),
            span_id: "span_def".to_string(),
            session_id: "session_ghi".to_string(),
            dataset_run_id: "ds_run_1".to_string(),
            name: "faithfulness".to_string(),
            value: 0.7,
            data_type: "NUMERIC".to_string(),
            string_value: String::new(),
            source: "EVAL".to_string(),
            comment: "A note".to_string(),
            author_user_id: "user_1".to_string(),
            config_id: "config_2".to_string(),
            eval_execution_trace_id: "eval_trace_3".to_string(),
            queue_id: "queue_4".to_string(),
            environment: "prod".to_string(),
            service_name: "evaluator".to_string(),
            agent_name: "planner".to_string(),
            user_id: "u_5".to_string(),
            metadata: r#"{"k":"v"}"#.to_string(),
            is_deleted: 0,
        };
        let batch = scores_to_record_batch(std::slice::from_ref(&s)).unwrap();
        let recovered = record_batch_to_scores(&batch).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0], s);
    }

    #[test]
    fn test_categorical_score_value_in_string_value() {
        let s = EvaluationScore {
            trace_id: "t1".to_string(),
            workspace_id: "p1".to_string(),
            name: "topic".to_string(),
            value: 0.0,
            data_type: "CATEGORICAL".to_string(),
            string_value: "support_request".to_string(),
            ..EvaluationScore::default()
        };
        let batch = scores_to_record_batch(std::slice::from_ref(&s)).unwrap();
        let recovered = record_batch_to_scores(&batch).unwrap();
        assert_eq!(recovered[0].data_type, "CATEGORICAL");
        assert_eq!(recovered[0].string_value, "support_request");
    }

    #[test]
    fn test_record_batch_to_scores_empty() {
        let batch = scores_to_record_batch(&[]).unwrap();
        let recovered = record_batch_to_scores(&batch).unwrap();
        assert!(recovered.is_empty());
    }
}
