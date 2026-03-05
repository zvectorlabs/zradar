//! OTLP Logs Service gRPC implementation with evaluation score extraction
//! and log record persistence.

use crate::auth::ApiKeyAuth;
use crate::logs_converter::OtlpLogsConverter;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_models::{EvaluationScore, RequestContext};

const SCORE_ATTRIBUTE_PREFIX: &str = "score.";

/// Callback trait for handling evaluation scores extracted from OTLP logs.
#[tonic::async_trait]
pub trait ScoreHandler: Send + Sync + 'static {
    /// Handle an evaluation score extracted from OTLP logs.
    async fn handle_score(
        &self,
        score: EvaluationScore,
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// Callback trait for persisting converted log records.
#[tonic::async_trait]
pub trait LogHandler: Send + Sync + 'static {
    /// Handle a batch of converted `LogRecord`s.
    async fn handle_logs(
        &self,
        logs: Vec<zradar_models::LogRecord>,
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// OTLP Logs Service implementation.
///
/// Processes each incoming request twice:
/// 1. Extracts evaluation scores (existing behaviour) via `ScoreHandler`.
/// 2. Persists all log records via `LogHandler`.
#[derive(Clone)]
pub struct OtlpLogsService<H: ScoreHandler, L: LogHandler> {
    score_handler: Arc<H>,
    log_handler: Arc<L>,
    auth: Option<Arc<ApiKeyAuth>>,
}

impl<H: ScoreHandler, L: LogHandler> OtlpLogsService<H, L> {
    /// Create a new `OtlpLogsService`.
    pub fn new(
        score_handler: Arc<H>,
        log_handler: Arc<L>,
        auth: Option<Arc<ApiKeyAuth>>,
    ) -> Self {
        Self {
            score_handler,
            log_handler,
            auth,
        }
    }

    async fn authenticate<T>(&self, request: &Request<T>) -> Result<RequestContext, Status> {
        if let Some(ref auth) = self.auth {
            auth.validate(request).await
        } else {
            Ok(RequestContext::default())
        }
    }

    /// Parse an evaluation score from a log record.
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
                "id" => {
                    score.id = get_string_value(value);
                }
                "trace_id" => {
                    score.trace_id = get_string_value(value);
                    if !score.trace_id.is_empty() {
                        has_required_fields = true;
                    }
                }
                "span_id" => {
                    score.span_id = get_string_value(value);
                }
                "session_id" => {
                    score.session_id = get_string_value(value);
                }
                "dataset_run_id" => {
                    score.dataset_run_id = get_string_value(value);
                }
                "name" => {
                    score.name = get_string_value(value);
                }
                "value" => {
                    score.value = get_double_value(value);
                }
                "data_type" => {
                    score.data_type = get_string_value(value);
                }
                "string_value" => {
                    score.string_value = get_string_value(value);
                }
                "source" => {
                    score.source = get_string_value(value);
                }
                "comment" => {
                    score.comment = get_string_value(value);
                }
                "author_user_id" => {
                    score.author_user_id = get_string_value(value);
                }
                "config_id" => {
                    score.config_id = get_string_value(value);
                }
                "eval_execution_trace_id" => {
                    score.eval_execution_trace_id = get_string_value(value);
                }
                "queue_id" => {
                    score.queue_id = get_string_value(value);
                }
                "environment" => {
                    score.environment = get_string_value(value);
                }
                "service_name" => {
                    score.service_name = get_string_value(value);
                }
                "agent_name" => {
                    score.agent_name = get_string_value(value);
                }
                "user_id" => {
                    score.user_id = get_string_value(value);
                }
                "metadata" => {
                    score.metadata = get_string_value(value);
                }
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

        if has_score_attrs
            && has_required_fields
            && !score.name.is_empty()
            && !score.trace_id.is_empty()
        {
            Some(score)
        } else {
            None
        }
    }
}

/// Extract string value from AnyValue.
fn get_string_value(value: &AnyValue) -> String {
    match value {
        AnyValue::StringValue(s) => s.clone(),
        AnyValue::IntValue(i) => i.to_string(),
        AnyValue::DoubleValue(d) => d.to_string(),
        AnyValue::BoolValue(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Extract double value from AnyValue.
fn get_double_value(value: &AnyValue) -> f64 {
    match value {
        AnyValue::DoubleValue(d) => *d,
        AnyValue::IntValue(i) => *i as f64,
        AnyValue::StringValue(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

#[tonic::async_trait]
impl<H: ScoreHandler, L: LogHandler> LogsService for OtlpLogsService<H, L> {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let context = self.authenticate(&request).await?;

        // Clone request data for dual use: score extraction + log persistence.
        let req = request.into_inner();

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_logs = req.resource_logs.len(),
            "Received logs export request"
        );

        // --- Score extraction (iterate raw OTLP records) ---
        let mut score_count = 0;
        for resource_logs in &req.resource_logs {
            for scope_logs in &resource_logs.scope_logs {
                for log_record in &scope_logs.log_records {
                    if let Some(score) = self.parse_score(log_record, &context) {
                        self.score_handler
                            .handle_score(score, &context)
                            .await?;
                        score_count += 1;
                    }
                }
            }
        }

        // --- Log persistence (convert then write) ---
        let logs = OtlpLogsConverter::convert(req, &context);
        let log_count = logs.len();
        self.log_handler.handle_logs(logs, &context).await?;

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
