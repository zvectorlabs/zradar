//! OTLP Metrics Service gRPC implementation.

use crate::auth::authenticate_grpc;
use crate::circuit_breaker::CircuitBreaker;
use crate::ingestion_guard::enforce_project_settings;
use crate::metrics_converter::OtlpMetricsConverter;
use crate::rate_limiter::ProjectRateLimiter;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_traits::{Authenticator, SettingsRepository, TelemetryWriter};

/// OTLP Metrics Service — converts OTLP protobuf to metrics and writes them.
#[derive(Clone)]
pub struct OtlpMetricsService {
    writer: Arc<dyn TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    settings_repo: Option<Arc<dyn SettingsRepository>>,
    rate_limiter: Option<Arc<ProjectRateLimiter>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl OtlpMetricsService {
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
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let context = authenticate_grpc(&self.auth, &request).await?;
        if let Some(circuit_breaker) = &self.circuit_breaker {
            circuit_breaker.check_status().await?;
        }
        let req = request.into_inner();

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_metrics = req.resource_metrics.len(),
            "Received metrics export request"
        );

        let metrics = OtlpMetricsConverter::convert(req, &context);
        enforce_project_settings(
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
                tenant_id = %context.tenant_id,
                project_id = %context.project_id,
                metrics = metrics.len(),
                "Persisted metrics"
            );
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
