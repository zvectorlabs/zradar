//! OTLP Trace Service gRPC implementation.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::converter::OtlpConverter;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_project_settings_and_get};
use crate::parser_caps::validate_trace_request;
use crate::rate_limiter::ProjectRateLimiter;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_policy::{PolicyEnforcer, SignalKind};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

/// OTLP Trace Service — converts OTLP protobuf to spans and writes them.
#[derive(Clone)]
pub struct OtlpTraceService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    allow_test_header_context: bool,
}

impl OtlpTraceService {
    pub fn new(writer: Arc<dyn TelemetryWriter>, auth: Option<Arc<dyn Authenticator>>) -> Self {
        Self {
            writer,
            auth,
            settings_repo: None,
            rate_limiter: None,
            policy_enforcer: None,
            circuit_breaker: None,
            allow_test_header_context: false,
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
            allow_test_header_context: false,
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
            allow_test_header_context: false,
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
            allow_test_header_context: false,
        }
    }

    pub fn with_test_header_context(mut self, allow: bool) -> Self {
        self.allow_test_header_context = allow;
        self
    }
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let context =
            authenticate_grpc(&self.auth, &request, self.allow_test_header_context).await?;
        if let Some(circuit_breaker) = &self.circuit_breaker {
            circuit_breaker.check_status().await?;
        }
        let req = request.into_inner();
        validate_trace_request(&req).map_err(|e| e.into_status())?;
        let span_count = req
            .resource_spans
            .iter()
            .flat_map(|resource_spans| &resource_spans.scope_spans)
            .map(|scope_spans| scope_spans.spans.len() as u64)
            .sum();
        if let Some(policy_enforcer) = &self.policy_enforcer {
            enforce_policy_ingest(
                policy_enforcer.as_ref(),
                &context,
                SignalKind::Traces,
                span_count,
                None,
            )
            .await?;
        }
        let settings = enforce_project_settings_and_get(
            &self.settings_repo,
            &self.rate_limiter,
            &context,
            span_count,
        )
        .await?;
        let capture_enabled = settings
            .as_ref()
            .map(|settings| settings.capture_llm_content_enabled)
            .unwrap_or(true);

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_spans = req.resource_spans.len(),
            "Received trace export request"
        );

        let converter = OtlpConverter::new().with_capture_enabled(capture_enabled);
        let mut all_spans = Vec::new();
        for resource_spans in req.resource_spans {
            let spans = converter
                .convert_resource_spans_with(resource_spans, &context)
                .map_err(|e| Status::internal(format!("Failed to convert spans: {}", e)))?;
            all_spans.extend(spans);
        }

        if !all_spans.is_empty() {
            self.writer
                .insert_spans(&all_spans)
                .await
                .map_err(|e| Status::internal(format!("Failed to insert spans: {}", e)))?;

            tracing::info!(
                tenant_id = %context.tenant_id,
                project_id = %context.project_id,
                spans = all_spans.len(),
                "Persisted spans"
            );
        }

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
