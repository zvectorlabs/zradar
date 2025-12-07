//! Generic worker implementation
//!
//! Works with any JobQueue and BlockStorage implementation

use std::sync::Arc;
use std::time::Duration;

use api_optel::OtlpConverter;
use zradar_models::RequestContext;
use zradar_traits::{BlockStorage, Job, JobQueue, TelemetryWriter};

/// Generic worker that processes jobs
///
/// Uses TelemetryWriter trait for flexibility - can be configured with:
/// - ClickHouseTelemetryWriter (production)
/// - Mock implementation (testing)
/// - Custom implementation (alternative storage)
pub struct Worker {
    id: String,
    job_queue: Arc<dyn JobQueue>,
    block_storage: Arc<dyn BlockStorage>,
    writer: Arc<dyn TelemetryWriter>,
}

impl Worker {
    /// Create new worker with any TelemetryWriter implementation
    pub fn new(
        id: String,
        job_queue: Arc<dyn JobQueue>,
        block_storage: Arc<dyn BlockStorage>,
        writer: Arc<dyn TelemetryWriter>,
    ) -> Self {
        Self {
            id,
            job_queue,
            block_storage,
            writer,
        }
    }

    /// Run worker loop
    pub async fn run(self: Arc<Self>) {
        tracing::info!(worker_id = %self.id, "Worker started");

        loop {
            match self.process_next_job().await {
                Ok(true) => {
                    // Processed a job, check for more immediately
                }
                Ok(false) => {
                    // No job available, short sleep
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    tracing::error!(
                        worker_id = %self.id,
                        error = %e,
                        "Worker error"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process_next_job(&self) -> anyhow::Result<bool> {
        // Dequeue job (implementation-agnostic!)
        let Some(job) = self.job_queue.dequeue(&self.id).await? else {
            return Ok(false);
        };

        tracing::info!(
            worker_id = %self.id,
            job_id = %job.id,
            tenant_id = %job.tenant_id,
            project_id = %job.project_id,
            "Processing job"
        );

        // Process job
        match self.process_job(&job).await {
            Ok(()) => {
                self.job_queue.complete(job.id).await?;

                // CLEANUP after successful ClickHouse insertion
                // - Local storage: Deletes file immediately
                // - S3: No-op (relies on S3 lifecycle policies for auto-deletion after 24h)
                if let Err(e) = self.block_storage.cleanup(&job.file_path).await {
                    tracing::warn!(
                        worker_id = %self.id,
                        job_id = %job.id,
                        error = %e,
                        "Failed to cleanup file after processing"
                    );
                }

                tracing::info!(
                    worker_id = %self.id,
                    job_id = %job.id,
                    "Job completed"
                );
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                self.job_queue.fail(job.id, &error_msg).await?;

                tracing::error!(
                    worker_id = %self.id,
                    job_id = %job.id,
                    error = %e,
                    "Job failed"
                );
            }
        }

        Ok(true)
    }

    async fn process_job(&self, job: &Job) -> anyhow::Result<()> {
        // Download from block storage
        let data = self.block_storage.download(&job.file_path).await?;

        tracing::debug!(
            worker_id = %self.id,
            job_id = %job.id,
            size = data.len(),
            "Downloaded job data"
        );

        // Deserialize OTLP protobuf
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use prost::Message;

        let request = ExportTraceServiceRequest::decode(&data[..])?;

        tracing::debug!(
            worker_id = %self.id,
            job_id = %job.id,
            resource_spans = request.resource_spans.len(),
            "Decoded OTLP request"
        );

        // Create context
        let context = RequestContext {
            tenant_id: job.tenant_id.clone(),
            project_id: job.project_id.clone(),
            permissions: vec![],
        };

        // Convert and insert
        let mut all_spans = Vec::new();
        for resource_spans in request.resource_spans {
            let spans = OtlpConverter::convert_resource_spans(resource_spans, &context)?;
            all_spans.extend(spans);
        }

        tracing::debug!(
            worker_id = %self.id,
            job_id = %job.id,
            spans = all_spans.len(),
            "Converted spans"
        );

        // Insert spans using writer trait (allows different implementations)
        if !all_spans.is_empty() {
            self.writer
                .insert_spans(&all_spans)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to insert spans: {}", e))?;

            tracing::info!(
                worker_id = %self.id,
                job_id = %job.id,
                spans = all_spans.len(),
                "Inserted spans via TelemetryWriter"
            );
        }

        Ok(())
    }
}

/// Worker pool manager
pub struct WorkerPool {
    workers: Vec<Arc<Worker>>,
}

impl WorkerPool {
    /// Create new worker pool with any TelemetryWriter implementation
    pub fn new(
        num_workers: usize,
        job_queue: Arc<dyn JobQueue>,
        block_storage: Arc<dyn BlockStorage>,
        writer: Arc<dyn TelemetryWriter>,
    ) -> Self {
        let workers = (0..num_workers)
            .map(|i| {
                Arc::new(Worker::new(
                    format!("worker-{}", i),
                    job_queue.clone(),
                    block_storage.clone(),
                    writer.clone(),
                ))
            })
            .collect();

        Self { workers }
    }

    /// Start all workers
    pub async fn start(&self) {
        for worker in &self.workers {
            let worker = worker.clone();
            tokio::spawn(async move {
                worker.run().await;
            });
        }

        tracing::info!(num_workers = self.workers.len(), "Worker pool started");
    }
}
