//! OTLP Metrics Service gRPC implementation

use crate::auth::ApiKeyAuth;
use crate::metrics_converter::OtlpMetricsConverter;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use zradar_models::RequestContext;

/// Callback trait for handling converted metrics.
#[tonic::async_trait]
pub trait MetricHandler: Send + Sync + 'static {
    async fn handle_metrics(
        &self,
        metrics: Vec<zradar_models::Metric>,
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// OTLP Metrics Service implementation.
#[derive(Clone)]
pub struct OtlpMetricsService<H: MetricHandler> {
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
        let context = self.authenticate(&request).await?;
        let req = request.into_inner();

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_metrics = req.resource_metrics.len(),
            "Received metrics export request"
        );

        let metrics = OtlpMetricsConverter::convert(req, &context);
        let metric_count = metrics.len();

        if !metrics.is_empty() {
            self.handler.handle_metrics(metrics, &context).await?;
        }

        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            metrics = metric_count,
            "Successfully processed metrics"
        );

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
