//! OTLP Logs Service gRPC implementation.
//!
//! Processes each incoming request:
//! 1. Extracts evaluation scores from log attributes (score.* prefix).
//! 2. Persists all log records via `TelemetryWriter`.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::ingestion_guard::enforce_project_settings;
use crate::logs_converter::OtlpLogsConverter;
use crate::rate_limiter::ProjectRateLimiter;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_models::{EvaluationScore, RequestContext};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

const SCORE_ATTRIBUTE_PREFIX: &str = "score.";

/// OTLP Logs Service — converts OTLP log records and writes them.
#[derive(Clone)]
pub struct OtlpLogsService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl OtlpLogsService {
    pub fn new(writer: Arc<dyn TelemetryWriter>, auth: Option<Arc<dyn Authenticator>>) -> Self {
        Self {
            writer,
            auth,
            settings_repo: None,
            rate_limiter: None,
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
            circuit_breaker: Some(circuit_breaker),
        }
    }

    fn parse_score(&self, log: &LogRecord, context: &RequestContext) -> Option<EvaluationScore> {
        let mut score = EvaluationScore {
            tenant_id: context.tenant_id.clone(),
            project_id: context.project_id.clone(),
            ..EvaluationScore::default()
        };

        let mut has_score_attrs = false;
        let mut has_required_fields = false;

        for attr in &log.attributes {
            let key = &attr.key;
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
                "user_id" => score.user_id = get_string_value(value),
                "metadata" => score.metadata = get_string_value(value),
                _ => {}
            }
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
                    if let Some(_score) = self.parse_score(log_record, &context) {
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
}
