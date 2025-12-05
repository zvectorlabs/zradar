//! Direct span handler - bypasses job queue for immediate persistence
//!
//! This handler converts OTLP protobuf spans directly to internal format
//! and writes them synchronously to the persistence layer, skipping the
//! job queue entirely. Useful for:
//! - Development/testing environments
//! - Low-volume deployments
//! - Scenarios where eventual consistency is not acceptable

use std::sync::Arc;
use zradar_traits::TelemetryWriter;
use zradar_models::RequestContext;
use tonic::Status;
use prost::Message;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use crate::{SpanHandler, MetricHandler, OtlpConverter};

/// Direct span handler that immediately persists spans
pub struct DirectSpanHandler {
    writer: Arc<dyn TelemetryWriter>,
}

impl DirectSpanHandler {
    /// Create new direct span handler
    pub fn new(writer: Arc<dyn TelemetryWriter>) -> Self {
        Self { writer }
    }
}

#[tonic::async_trait]
impl SpanHandler for DirectSpanHandler {
    async fn handle_raw_otlp(
        &self,
        data: &[u8],
        context: &RequestContext,
    ) -> Result<(), Status> {
        // Deserialize OTLP protobuf
        let request = ExportTraceServiceRequest::decode(data)
            .map_err(|e| Status::internal(format!("Failed to decode OTLP request: {}", e)))?;
        
        // Convert to internal format
        let mut all_spans = Vec::new();
        for resource_spans in request.resource_spans {
            let spans = OtlpConverter::convert_resource_spans(resource_spans, context)
                .map_err(|e| Status::internal(format!("Failed to convert spans: {}", e)))?;
            all_spans.extend(spans);
        }
        
        tracing::debug!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            spans = all_spans.len(),
            "Converted spans for direct write"
        );
        
        // Write directly to persistence layer
        if !all_spans.is_empty() {
            self.writer.insert_spans(&all_spans)
                .await
                .map_err(|e| Status::internal(format!("Failed to insert spans: {}", e)))?;
            
            tracing::info!(
                tenant_id = %context.tenant_id,
                project_id = %context.project_id,
                spans = all_spans.len(),
                "Directly persisted spans (bypassed job queue)"
            );
        }
        
        Ok(())
    }
}

#[tonic::async_trait]
impl MetricHandler for DirectSpanHandler {
    async fn handle_metrics(
        &self,
        _metrics: Vec<zradar_models::Metric>,
        _context: &RequestContext,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "Metric ingestion via direct handler not yet implemented"
        ))
    }
}

