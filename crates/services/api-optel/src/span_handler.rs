//! Job queue span handler - bridges OTLP to job queue

use std::sync::Arc;
use zradar_traits::JobQueue;
use zradar_models::RequestContext;
use tonic::Status;

use crate::SpanHandler;
use crate::MetricHandler;

/// Wrapper to bridge JobQueue to SpanHandler and MetricHandler traits
/// 
/// This allows OTLP services to enqueue data for asynchronous processing
/// rather than inserting directly into storage.
pub struct JobQueueSpanHandler {
    queue: Arc<dyn JobQueue>,
}

impl JobQueueSpanHandler {
    /// Create new job queue span handler
    pub fn new(queue: Arc<dyn JobQueue>) -> Self {
        Self { queue }
    }
    
    /// Get reference to the underlying job queue
    pub fn queue(&self) -> &Arc<dyn JobQueue> {
        &self.queue
    }
}

#[tonic::async_trait]
impl SpanHandler for JobQueueSpanHandler {
    async fn handle_raw_otlp(
        &self,
        data: &[u8],
        context: &RequestContext,
    ) -> Result<(), Status> {
        self.queue.enqueue(data, context)
            .await
            .map(|_job_id| ())
            .map_err(|e| Status::internal(format!("Failed to enqueue job: {}", e)))
    }
}

#[tonic::async_trait]
impl MetricHandler for JobQueueSpanHandler {
    async fn handle_metrics(
        &self,
        _metrics: Vec<zradar_models::Metric>,
        _context: &RequestContext,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "Metric ingestion via job queue not yet implemented"
        ))
    }
}
