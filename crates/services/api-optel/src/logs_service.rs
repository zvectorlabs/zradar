//! OTLP Logs Service gRPC implementation with evaluation score extraction

use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService,
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use tonic::{Request, Response, Status};
use std::sync::Arc;
use zradar_models::{RequestContext, EvaluationScore};
use crate::auth::ApiKeyAuth;

const SCORE_ATTRIBUTE_PREFIX: &str = "score.";

/// Callback trait for handling evaluation scores from OTLP logs
#[tonic::async_trait]
pub trait ScoreHandler: Send + Sync + 'static {
    /// Handle an evaluation score extracted from OTLP logs
    /// 
    /// # Arguments
    /// * `score` - The evaluation score
    /// * `context` - Request context (tenant_id, project_id, etc.)
    async fn handle_score(
        &self,
        score: EvaluationScore,
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// OTLP Logs Service implementation
#[derive(Clone)]
pub struct OtlpLogsService<H: ScoreHandler> {
    handler: Arc<H>,
    auth: Option<Arc<ApiKeyAuth>>,
}

impl<H: ScoreHandler> OtlpLogsService<H> {
    pub fn new(handler: Arc<H>, auth: Option<Arc<ApiKeyAuth>>) -> Self {
        Self { handler, auth }
    }
    
    async fn authenticate<T>(&self, request: &Request<T>) -> Result<RequestContext, Status> {
        if let Some(ref auth) = self.auth {
            auth.validate(request).await
        } else {
            // No auth - use default context
            Ok(RequestContext::default())
        }
    }
    
    /// Parse an evaluation score from a log record
    fn parse_score(&self, log: &LogRecord, context: &RequestContext) -> Option<EvaluationScore> {
        let mut score = EvaluationScore::default();
        score.tenant_id = context.tenant_id.clone();
        score.project_id = context.project_id.clone();
        
        let mut has_score_attrs = false;
        let mut has_required_fields = false;
        
        // Extract attributes
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
                _ => {
                    // Unknown score attribute, ignore
                }
            }
        }
        
        // Set timestamps
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        score.timestamp = log.time_unix_nano as i64;
        score.created_at = now;
        score.updated_at = now;
        score.event_ts = now;
        
        // Generate ID if not provided
        if score.id.is_empty() {
            score.id = format!("eval_{}", uuid::Uuid::new_v4());
        }
        
        // Validate required fields
        if has_score_attrs && has_required_fields && !score.name.is_empty() && !score.trace_id.is_empty() {
            Some(score)
        } else {
            None
        }
    }
}

/// Extract string value from AnyValue
fn get_string_value(value: &AnyValue) -> String {
    match value {
        AnyValue::StringValue(s) => s.clone(),
        AnyValue::IntValue(i) => i.to_string(),
        AnyValue::DoubleValue(d) => d.to_string(),
        AnyValue::BoolValue(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Extract double value from AnyValue
fn get_double_value(value: &AnyValue) -> f64 {
    match value {
        AnyValue::DoubleValue(d) => *d,
        AnyValue::IntValue(i) => *i as f64,
        AnyValue::StringValue(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

#[tonic::async_trait]
impl<H: ScoreHandler> LogsService for OtlpLogsService<H> {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        // Authenticate
        let context = self.authenticate(&request).await?;
        
        let req = request.into_inner();
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_logs = req.resource_logs.len(),
            "Received logs export request"
        );
        
        let mut score_count = 0;
        
        // Process each log record
        for resource_logs in req.resource_logs {
            for scope_logs in resource_logs.scope_logs {
                for log_record in scope_logs.log_records {
                    // Try to parse as evaluation score
                    if let Some(score) = self.parse_score(&log_record, &context) {
                        self.handler.handle_score(score, &context).await?;
                        score_count += 1;
                    }
                }
            }
        }
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            scores_extracted = score_count,
            "Successfully processed logs"
        );
        
        // Return success response
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
        assert_eq!(get_string_value(&AnyValue::StringValue("test".to_string())), "test");
        assert_eq!(get_string_value(&AnyValue::IntValue(42)), "42");
        assert_eq!(get_string_value(&AnyValue::DoubleValue(3.14)), "3.14");
        assert_eq!(get_string_value(&AnyValue::BoolValue(true)), "true");
    }

    #[test]
    fn test_get_double_value() {
        assert_eq!(get_double_value(&AnyValue::DoubleValue(3.14)), 3.14);
        assert_eq!(get_double_value(&AnyValue::IntValue(42)), 42.0);
        assert_eq!(get_double_value(&AnyValue::StringValue("3.14".to_string())), 3.14);
    }

    #[test]
    fn test_score_attribute_prefix() {
        assert_eq!(SCORE_ATTRIBUTE_PREFIX, "score.");
    }
}

