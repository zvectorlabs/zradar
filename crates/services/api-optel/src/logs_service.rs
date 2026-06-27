//! OTLP Logs Service gRPC implementation.
//!
//! Processes each incoming request:
//! 1. Extracts evaluation scores from log attributes (score.* prefix).
//! 2. Persists all log records via `TelemetryWriter`.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_project_settings};
use crate::logs_converter::OtlpLogsConverter;
use crate::parser_caps::validate_logs_request;
use crate::rate_limiter::ProjectRateLimiter;
use crate::score_extractor::extract_scores;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_policy::{PolicyEnforcer, SignalKind};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

/// OTLP Logs Service — converts OTLP log records and writes them.
#[derive(Clone)]
pub struct OtlpLogsService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    allow_test_header_context: bool,
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
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let context =
            authenticate_grpc(&self.auth, &request, self.allow_test_header_context).await?;
        if let Some(circuit_breaker) = &self.circuit_breaker {
            circuit_breaker.check_status().await?;
        }
        let req = request.into_inner();
        validate_logs_request(&req).map_err(|e| e.into_status())?;
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

        // Score extraction: collect evaluation scores from log attributes whose
        // keys start with `score.` and persist them through the TelemetryWriter
        // (Phase 1 R1.8 / OQ8). Scores ride the same WAL + Parquet pipeline as
        // traces/metrics/logs. Uses the shared extractor so HTTP transport gets
        // identical behaviour (see score_extractor.rs).
        let scores = extract_scores(&req, &context);
        let score_count = scores.len();

        if !scores.is_empty() {
            self.writer
                .insert_scores(&scores)
                .await
                .map_err(|e| Status::internal(format!("Failed to insert scores: {}", e)))?;
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

// Score extraction tests live in score_extractor.rs alongside the implementation.
