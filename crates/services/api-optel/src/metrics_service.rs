//! OTLP Metrics Service gRPC implementation.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_workspace_settings};
use crate::metrics_converter::OtlpMetricsConverter;
use crate::parser_caps::validate_metrics_request;
use crate::rate_limiter::ProjectRateLimiter;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_policy::{PolicyEnforcer, SignalKind};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

/// OTLP Metrics Service — converts OTLP protobuf to metrics and writes them.
#[derive(Clone)]
pub struct OtlpMetricsService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    allow_test_header_context: bool,
}

impl OtlpMetricsService {
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
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let context =
            authenticate_grpc(&self.auth, &request, self.allow_test_header_context).await?;
        if let Some(circuit_breaker) = &self.circuit_breaker {
            circuit_breaker.check_status().await?;
        }
        let req = request.into_inner();
        validate_metrics_request(&req).map_err(|e| e.into_status())?;

        tracing::debug!(
            workspace_id = %context.workspace_id,
            resource_metrics = req.resource_metrics.len(),
            "Received metrics export request"
        );

        let metrics = OtlpMetricsConverter::convert(req, &context);
        if let Some(policy_enforcer) = &self.policy_enforcer {
            enforce_policy_ingest(
                policy_enforcer.as_ref(),
                &context,
                SignalKind::Metrics,
                metrics.len() as u64,
                None,
            )
            .await?;
        }
        enforce_workspace_settings(
            &self.settings_repo,
            &self.rate_limiter,
            &context,
            metrics.len() as u64,
        )
        .await?;

        if !metrics.is_empty() {
            self.writer
                .insert_metrics(&metrics)
                .await
                .map_err(|e| Status::internal(format!("Failed to insert metrics: {}", e)))?;

            tracing::info!(
                workspace_id = %context.workspace_id,
                metrics = metrics.len(),
                "Persisted metrics"
            );
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
