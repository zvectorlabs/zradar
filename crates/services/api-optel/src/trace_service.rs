//! OTLP Trace Service gRPC implementation

use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService,
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};
use std::sync::Arc;
use zradar_models::RequestContext;
use crate::auth::ApiKeyAuth;

/// Callback trait for handling raw OTLP trace data
#[tonic::async_trait]
pub trait SpanHandler: Send + Sync + 'static {
    /// Handle raw OTLP trace data
    /// 
    /// # Arguments
    /// * `data` - Serialized ExportTraceServiceRequest protobuf
    /// * `context` - Request context (tenant_id, project_id, etc.)
    async fn handle_raw_otlp(
        &self,
        data: &[u8],
        context: &RequestContext,
    ) -> Result<(), Status>;
}

/// OTLP Trace Service implementation
#[derive(Clone)]
pub struct OtlpTraceService<H: SpanHandler> {
    handler: Arc<H>,
    auth: Option<Arc<ApiKeyAuth>>,
}

impl<H: SpanHandler> OtlpTraceService<H> {
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
}

#[tonic::async_trait]
impl<H: SpanHandler> TraceService for OtlpTraceService<H> {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        // Authenticate
        let context = self.authenticate(&request).await?;
        
        let req = request.into_inner();
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            resource_spans = req.resource_spans.len(),
            "Received trace export request"
        );
        
        // Serialize OTLP request to bytes (for storage)
        use prost::Message;
        let mut buf = Vec::new();
        req.encode(&mut buf)
            .map_err(|e| Status::internal(format!("Failed to serialize OTLP request: {}", e)))?;
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            size = buf.len(),
            "Serialized OTLP request"
        );
        
        // Pass raw bytes to handler (for job queue)
        self.handler.handle_raw_otlp(&buf, &context).await?;
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            "Successfully enqueued trace data"
        );
        
        // Return success response
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

