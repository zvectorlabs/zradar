//! Shared evaluation-score extraction from OTLP log records (R1.8).
//!
//! Used by both the gRPC `/logs` path (`OtlpLogsService`) and the HTTP
//! `/v1/logs` handler so both transports persist evaluation scores through
//! the same WAL + Parquet pipeline.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use serde_json::Value;
use zradar_models::{EvaluationScore, RequestContext};

const SCORE_ATTRIBUTE_PREFIX: &str = "score.";

/// Extract all evaluation scores from every log record in a request.
///
/// A log record is treated as a score event when it carries attributes
/// whose keys start with `score.`, the record includes a non-empty
/// `score.trace_id`, and a non-empty `score.name`. Records that do not
/// match are silently skipped.
pub fn extract_scores(
    req: &ExportLogsServiceRequest,
    context: &RequestContext,
) -> Vec<EvaluationScore> {
    let mut scores = Vec::new();
    for resource_logs in &req.resource_logs {
        for scope_logs in &resource_logs.scope_logs {
            for log_record in &scope_logs.log_records {
                if let Some(score) = parse_score(log_record, context) {
                    scores.push(score);
                }
            }
        }
    }
    scores
}

fn parse_score(log: &LogRecord, context: &RequestContext) -> Option<EvaluationScore> {
    let mut score = EvaluationScore {
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        ..EvaluationScore::default()
    };

    let mut has_score_attrs = false;
    let mut has_required_fields = false;
    let mut skills = Vec::new();

    for attr in &log.attributes {
        let key = attr.key.as_str();
        if !key.starts_with(SCORE_ATTRIBUTE_PREFIX) {
            continue;
        }
        has_score_attrs = true;
        let field = &key[SCORE_ATTRIBUTE_PREFIX.len()..];
        // Skip a malformed (null/missing-value) attribute rather than aborting
        // the whole score — one bad `score.*` attr must not drop the record.
        let Some(value) = attr.value.as_ref().and_then(|v| v.value.as_ref()) else {
            continue;
        };

        match field {
            "id" => score.id = get_string_value(value),
            "trace_id" => {
                score.trace_id = get_string_value(value);
                if !score.trace_id.is_empty() {
                    has_required_fields = true;
                }
            }
            "span_id" => score.span_id = get_string_value(value),
            "session_id" => score.session_id = get_string_value(value),
            "dataset_run_id" => score.dataset_run_id = get_string_value(value),
            "name" => score.name = get_string_value(value),
            "value" => score.value = get_double_value(value),
            "data_type" => score.data_type = get_string_value(value),
            "string_value" => score.string_value = get_string_value(value),
            "source" => score.source = get_string_value(value),
            "comment" => score.comment = get_string_value(value),
            "author_user_id" => score.author_user_id = get_string_value(value),
            "config_id" => score.config_id = get_string_value(value),
            "eval_execution_trace_id" => score.eval_execution_trace_id = get_string_value(value),
            "queue_id" => score.queue_id = get_string_value(value),
            "environment" => score.environment = get_string_value(value),
            "service_name" => score.service_name = get_string_value(value),
            "agent_name" => score.agent_name = get_string_value(value),
            "skills" | "agent_skills" | "skill" => skills.extend(get_skill_values(value)),
            "user_id" => score.user_id = get_string_value(value),
            "metadata" => score.metadata = get_string_value(value),
            _ => {}
        }
    }

    if !skills.is_empty() {
        score.metadata = merge_skills_into_metadata(&score.metadata, &skills);
    }

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    score.timestamp = log.time_unix_nano as i64;
    score.created_at = now;
    score.updated_at = now;
    score.event_ts = now;

    if score.id.is_empty() {
        score.id = format!("eval_{}", uuid::Uuid::new_v4());
    }

    if has_score_attrs && has_required_fields && !score.name.is_empty() {
        Some(score)
    } else {
        None
    }
}

fn get_string_value(value: &AnyValue) -> String {
    match value {
        AnyValue::StringValue(s) => s.clone(),
        AnyValue::IntValue(i) => i.to_string(),
        AnyValue::DoubleValue(d) => d.to_string(),
        AnyValue::BoolValue(b) => b.to_string(),
        _ => String::new(),
    }
}

fn get_double_value(value: &AnyValue) -> f64 {
    match value {
        AnyValue::DoubleValue(d) => *d,
        AnyValue::IntValue(i) => *i as f64,
        AnyValue::StringValue(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn get_skill_values(value: &AnyValue) -> Vec<String> {
    match value {
        AnyValue::StringValue(s) => parse_skill_string(s),
        AnyValue::ArrayValue(array) => array
            .values
            .iter()
            .filter_map(|value| value.value.as_ref())
            .flat_map(get_skill_values)
            .collect(),
        _ => {
            let value = get_string_value(value);
            if value.is_empty() {
                Vec::new()
            } else {
                vec![value]
            }
        }
    }
}

fn parse_skill_string(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if let Ok(parsed) = serde_json::from_str::<Vec<String>>(trimmed) {
        return parsed
            .into_iter()
            .map(|skill| skill.trim().to_string())
            .filter(|skill| !skill.is_empty())
            .collect();
    }
    trimmed
        .split(',')
        .map(|skill| skill.trim().to_string())
        .filter(|skill| !skill.is_empty())
        .collect()
}

fn merge_skills_into_metadata(metadata: &str, skills: &[String]) -> String {
    let mut metadata = serde_json::from_str::<Value>(metadata)
        .ok()
        .filter(|value| value.is_object())
        .unwrap_or_else(|| serde_json::json!({}));

    if let Some(object) = metadata.as_object_mut() {
        object.insert(
            "skills".to_string(),
            Value::Array(skills.iter().cloned().map(Value::String).collect()),
        );
    }

    serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::LogRecord;

    fn kv_str(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::StringValue(v.to_string())),
            }),
        }
    }

    fn kv_f64(k: &str, v: f64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::DoubleValue(v)),
            }),
        }
    }

    fn make_log(attrs: Vec<KeyValue>) -> LogRecord {
        LogRecord {
            attributes: attrs,
            ..Default::default()
        }
    }

    fn ctx() -> RequestContext {
        RequestContext {
            tenant_id: "tenant-1".to_string(),
            project_id: "proj-1".to_string(),
        }
    }

    #[test]
    fn test_extract_scores_valid_record() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![make_log(vec![
                        kv_str("score.trace_id", "abc123"),
                        kv_str("score.name", "accuracy"),
                        kv_f64("score.value", 0.95),
                    ])],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let scores = extract_scores(&req, &ctx());
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].trace_id, "abc123");
        assert_eq!(scores[0].name, "accuracy");
        assert!((scores[0].value - 0.95).abs() < 1e-9);
        assert_eq!(scores[0].tenant_id, "tenant-1");
        assert_eq!(scores[0].project_id, "proj-1");
    }

    #[test]
    fn test_extract_scores_skips_records_without_score_prefix() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![make_log(vec![kv_str("message", "hello")])],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let scores = extract_scores(&req, &ctx());
        assert!(scores.is_empty());
    }

    #[test]
    fn test_extract_scores_requires_trace_id_and_name() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

        // Has score prefix but missing trace_id — should be skipped.
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![make_log(vec![kv_str("score.name", "accuracy")])],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert!(extract_scores(&req, &ctx()).is_empty());
    }

    #[test]
    fn test_extract_scores_skips_null_attr_without_aborting() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

        // A null-valued `score.*` attribute appears BEFORE the required fields.
        // With the old `?` early-abort this dropped the entire score; now the bad
        // attr is skipped and the record still parses.
        let null_attr = KeyValue {
            key: "score.comment".to_string(),
            value: Some(OtlpAnyValue { value: None }),
        };
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![make_log(vec![
                        null_attr,
                        kv_str("score.trace_id", "abc123"),
                        kv_str("score.name", "accuracy"),
                        kv_f64("score.value", 0.95),
                    ])],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let scores = extract_scores(&req, &ctx());
        assert_eq!(
            scores.len(),
            1,
            "null attr must be skipped, not abort the score"
        );
        assert_eq!(scores[0].trace_id, "abc123");
        assert_eq!(scores[0].name, "accuracy");
        assert!(
            scores[0].comment.is_empty(),
            "the skipped null attr leaves its field at default"
        );
    }
}
