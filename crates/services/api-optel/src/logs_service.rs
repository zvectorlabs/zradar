//! OTLP Logs Service gRPC implementation.
//!
//! Processes each incoming request:
//! 1. Extracts evaluation scores from log attributes (score.* prefix).
//! 2. Persists all log records via `TelemetryWriter`.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_project_settings};
use crate::logs_converter::OtlpLogsConverter;
use crate::rate_limiter::ProjectRateLimiter;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use serde_json::Value;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_models::{EvaluationScore, RequestContext};
use zradar_policy::{PolicyEnforcer, SignalKind};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

const SCORE_ATTRIBUTE_PREFIX: &str = "score.";

/// OTLP Logs Service — converts OTLP log records and writes them.
#[derive(Clone)]
pub struct OtlpLogsService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl OtlpLogsService {
    pub fn new(writer: Arc<dyn TelemetryWriter>, auth: Option<Arc<dyn Authenticator>>) -> Self {
        Self {
            writer,
            auth,
            settings_repo: None,
            rate_limiter: None,
            policy_enforcer: None,
            circuit_breaker: None,
        }
    }

    pub fn with_settings_repository(
        writer: Arc<dyn TelemetryWriter>,
        auth: Option<Arc<dyn Authenticator>>,
        settings_repo: Arc<dyn SettingsRepository>,
        rate_limiter: Arc<ProjectRateLimiter>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        Self {
            writer,
            auth,
            settings_repo: Some(settings_repo),
            rate_limiter: Some(rate_limiter),
            policy_enforcer: None,
            circuit_breaker: Some(circuit_breaker),
        }
    }

    pub fn with_policy_enforcer(
        writer: Arc<dyn TelemetryWriter>,
        auth: Option<Arc<dyn Authenticator>>,
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        Self {
            writer,
            auth,
            settings_repo: None,
            rate_limiter: None,
            policy_enforcer: Some(policy_enforcer),
            circuit_breaker: Some(circuit_breaker),
        }
    }

    pub fn with_settings_and_policy(
        writer: Arc<dyn TelemetryWriter>,
        auth: Option<Arc<dyn Authenticator>>,
        settings_repo: Arc<dyn SettingsRepository>,
        rate_limiter: Arc<ProjectRateLimiter>,
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        Self {
            writer,
            auth,
            settings_repo: Some(settings_repo),
            rate_limiter: Some(rate_limiter),
            policy_enforcer: Some(policy_enforcer),
            circuit_breaker: Some(circuit_breaker),
        }
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
            let value = attr.value.as_ref()?.value.as_ref()?;

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
                "eval_execution_trace_id" => {
                    score.eval_execution_trace_id = get_string_value(value)
                }
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

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let context = authenticate_grpc(&self.auth, &request).await?;
        if let Some(circuit_breaker) = &self.circuit_breaker {
            circuit_breaker.check_status().await?;
        }
        let req = request.into_inner();
        let raw_log_count = req
            .resource_logs
            .iter()
            .flat_map(|resource_logs| &resource_logs.scope_logs)
            .map(|scope_logs| scope_logs.log_records.len() as u64)
            .sum();
        if let Some(policy_enforcer) = &self.policy_enforcer {
            enforce_policy_ingest(
                policy_enforcer.as_ref(),
                &context,
                SignalKind::Logs,
                raw_log_count,
                None,
            )
            .await?;
        }
        enforce_project_settings(
            &self.settings_repo,
            &self.rate_limiter,
            &context,
            raw_log_count,
        )
        .await?;

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_logs = req.resource_logs.len(),
            "Received logs export request"
        );

        // Score extraction (iterate raw OTLP records — scores are not persisted here,
        // just logged for now since the scores service was removed)
        let mut score_count = 0;
        for resource_logs in &req.resource_logs {
            for scope_logs in &resource_logs.scope_logs {
                for log_record in &scope_logs.log_records {
                    if let Some(_score) = Self::parse_score(log_record, &context) {
                        score_count += 1;
                        // Score persistence can be added back when needed
                    }
                }
            }
        }

        // Log persistence
        let logs = OtlpLogsConverter::convert(req, &context);
        let log_count = logs.len();

        if !logs.is_empty() {
            self.writer
                .insert_logs(&logs)
                .await
                .map_err(|e| Status::internal(format!("Failed to insert logs: {}", e)))?;
        }

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            scores_extracted = score_count,
            logs_persisted = log_count,
            "Successfully processed logs"
        );

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, ArrayValue, KeyValue};

    fn score_attr(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: format!("score.{key}"),
            value: Some(OtlpAnyValue { value: Some(value) }),
        }
    }

    fn parse_score_with_attrs(attrs: Vec<KeyValue>) -> EvaluationScore {
        let log = LogRecord {
            time_unix_nano: 123,
            attributes: attrs,
            ..Default::default()
        };
        OtlpLogsService::parse_score(&log, &RequestContext::default()).expect("score should parse")
    }

    #[test]
    fn test_get_string_value() {
        assert_eq!(
            get_string_value(&AnyValue::StringValue("test".to_string())),
            "test"
        );
        assert_eq!(get_string_value(&AnyValue::IntValue(42)), "42");
        assert_eq!(get_string_value(&AnyValue::DoubleValue(1.23)), "1.23");
        assert_eq!(get_string_value(&AnyValue::BoolValue(true)), "true");
    }

    #[test]
    fn test_get_double_value() {
        assert_eq!(get_double_value(&AnyValue::DoubleValue(1.23)), 1.23);
        assert_eq!(get_double_value(&AnyValue::IntValue(42)), 42.0);
        assert_eq!(
            get_double_value(&AnyValue::StringValue("1.23".to_string())),
            1.23
        );
    }

    #[test]
    fn test_score_attribute_prefix() {
        assert_eq!(SCORE_ATTRIBUTE_PREFIX, "score.");
    }

    #[test]
    fn test_parse_score_merges_csv_skills_into_metadata() {
        let score = parse_score_with_attrs(vec![
            score_attr("trace_id", AnyValue::StringValue("trace-1".to_string())),
            score_attr("name", AnyValue::StringValue("quality".to_string())),
            score_attr(
                "metadata",
                AnyValue::StringValue(r#"{"source":"agent"}"#.to_string()),
            ),
            score_attr(
                "agent_skills",
                AnyValue::StringValue("search, summarize, classify".to_string()),
            ),
        ]);

        let metadata: serde_json::Value = serde_json::from_str(&score.metadata).unwrap();
        assert_eq!(metadata["source"], "agent");
        assert_eq!(
            metadata["skills"],
            serde_json::json!(["search", "summarize", "classify"])
        );
    }

    #[test]
    fn test_parse_score_merges_array_skills_into_metadata() {
        let score = parse_score_with_attrs(vec![
            score_attr("trace_id", AnyValue::StringValue("trace-1".to_string())),
            score_attr("name", AnyValue::StringValue("quality".to_string())),
            score_attr(
                "skills",
                AnyValue::ArrayValue(ArrayValue {
                    values: vec![
                        OtlpAnyValue {
                            value: Some(AnyValue::StringValue("search".to_string())),
                        },
                        OtlpAnyValue {
                            value: Some(AnyValue::StringValue("summarize".to_string())),
                        },
                    ],
                }),
            ),
        ]);

        let metadata: serde_json::Value = serde_json::from_str(&score.metadata).unwrap();
        assert_eq!(
            metadata["skills"],
            serde_json::json!(["search", "summarize"])
        );
    }
}
