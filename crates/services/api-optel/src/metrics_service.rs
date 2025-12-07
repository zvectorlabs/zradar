//! OTLP Metrics Service gRPC implementation

use crate::auth::ApiKeyAuth;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_models::RequestContext;

/// Callback trait for handling metrics
#[tonic::async_trait]
pub trait MetricHandler: Send + Sync + 'static {
    async fn handle_metrics(
        &self,
        metrics: Vec<zradar_models::Metric>,
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// OTLP Metrics Service implementation
#[derive(Clone)]
pub struct OtlpMetricsService<H: MetricHandler> {
    #[allow(dead_code)]
    handler: Arc<H>,
    auth: Option<Arc<ApiKeyAuth>>,
}

impl<H: MetricHandler> OtlpMetricsService<H> {
    pub fn new(handler: Arc<H>, auth: Option<Arc<ApiKeyAuth>>) -> Self {
        Self { handler, auth }
    }

    async fn authenticate<T>(&self, request: &Request<T>) -> Result<RequestContext, Status> {
        if let Some(ref auth) = self.auth {
            auth.validate(request).await
        } else {
            Ok(RequestContext::default())
        }
    }
}

#[tonic::async_trait]
impl<H: MetricHandler> MetricsService for OtlpMetricsService<H> {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        // Authenticate
        let context = self.authenticate(&request).await?;

        let req = request.into_inner();

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_metrics = req.resource_metrics.len(),
            "Received metrics export request"
        );

        // TODO: Implement metrics conversion
        // For now, just acknowledge receipt

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            "Successfully processed metrics (conversion not yet implemented)"
        );

        // Return success response
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
