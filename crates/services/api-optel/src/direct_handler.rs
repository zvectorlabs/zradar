//! Direct span/metric/log handlers - bypass job queue for immediate persistence.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use std::sync::Arc;
use tonic::Status;
use zradar_models::{EvaluationScore, RequestContext};
use zradar_traits::TelemetryWriter;

use crate::logs_converter::OtlpLogsConverter;
use crate::metrics_converter::OtlpMetricsConverter;
use crate::{LogHandler, MetricHandler, OtlpConverter, ScoreHandler, SpanHandler};

/// Direct span handler that immediately persists spans.
pub struct DirectSpanHandler {
    writer: Arc<dyn TelemetryWriter>,
}

impl DirectSpanHandler {
    /// Create new direct span handler.
    pub fn new(writer: Arc<dyn TelemetryWriter>) -> Self {
        Self { writer }
    }
}

#[tonic::async_trait]
impl SpanHandler for DirectSpanHandler {
    async fn handle_raw_otlp(&self, data: &[u8], context: &RequestContext) -> Result<(), Status> {
        let request = ExportTraceServiceRequest::decode(data)
            .map_err(|e| Status::internal(format!("Failed to decode OTLP request: {}", e)))?;


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

        if !all_spans.is_empty() {
            self.writer
                .insert_spans(&all_spans)
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
        metrics: Vec<zradar_models::Metric>,
        context: &RequestContext,
    ) -> Result<(), Status> {
        if metrics.is_empty() {
            return Ok(());
        }

        self.writer
            .insert_metrics(&metrics)
            .await
            .map_err(|e| Status::internal(format!("Failed to insert metrics: {}", e)))?;

        tracing::info!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            metrics = metrics.len(),
            "Directly persisted metrics"
        );

        Ok(())
    }
}

/// Direct log handler that immediately persists logs and extracts scores.
pub struct DirectLogHandler {
    writer: Arc<dyn TelemetryWriter>,
}

impl DirectLogHandler {
    /// Create new direct log handler.
    pub fn new(writer: Arc<dyn TelemetryWriter>) -> Self {
        Self { writer }
    }
}

#[tonic::async_trait]
impl LogHandler for DirectLogHandler {
    async fn handle_logs(
        &self,
        logs: Vec<zradar_models::LogRecord>,
        context: &RequestContext,
    ) -> Result<(), Status> {
        if logs.is_empty() {
            return Ok(());
        }

        self.writer
            .insert_logs(&logs)
            .await
            .map_err(|e| Status::internal(format!("Failed to insert logs: {}", e)))?;

        tracing::info!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            logs = logs.len(),
            "Directly persisted logs"
        );

        Ok(())
    }
}

/// A no-op score handler used when log persistence does not need score writing.
pub struct NullScoreHandler;

#[tonic::async_trait]
impl ScoreHandler for NullScoreHandler {
    async fn handle_score(
        &self,
        _score: EvaluationScore,
        _context: &RequestContext,
    ) -> Result<(), Status> {
        Ok(())
    }
}

/// A combined direct handler for the logs gRPC service.
///
/// Converts OTLP log records to `LogRecord` via `OtlpLogsConverter` and
/// persists them via `TelemetryWriter`.  Score extraction is performed by
/// the `OtlpLogsService` layer above this handler.
pub struct DirectLogsHandler {
    writer: Arc<dyn TelemetryWriter>,
}

impl DirectLogsHandler {
    /// Create a new handler.
    pub fn new(writer: Arc<dyn TelemetryWriter>) -> Self {
        Self { writer }
    }

    /// Convert and persist an `ExportLogsServiceRequest`.
    pub async fn persist_logs(
        &self,
        request: ExportLogsServiceRequest,
        context: &RequestContext,
    ) -> Result<(), Status> {
        let logs = OtlpLogsConverter::convert(request, context);
        if logs.is_empty() {
            return Ok(());
        }
        self.writer
            .insert_logs(&logs)
            .await
            .map_err(|e| Status::internal(format!("Failed to insert logs: {}", e)))?;

        tracing::info!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            logs = logs.len(),
            "Directly persisted logs"
        );
        Ok(())
    }
}

/// A no-op log handler used when log persistence is not needed.
pub struct NullLogHandler;

#[tonic::async_trait]
impl LogHandler for NullLogHandler {
    async fn handle_logs(
        &self,
        _logs: Vec<zradar_models::LogRecord>,
        _context: &RequestContext,
    ) -> Result<(), Status> {
        Ok(())
    }
}

/// Convert raw OTLP metrics bytes and persist them immediately.
pub struct DirectMetricsHandler {
    writer: Arc<dyn TelemetryWriter>,
}

impl DirectMetricsHandler {
    /// Create a new handler.
    pub fn new(writer: Arc<dyn TelemetryWriter>) -> Self {
        Self { writer }
    }

    /// Convert and persist an `ExportMetricsServiceRequest`.
    pub async fn persist_metrics(
        &self,
        request: ExportMetricsServiceRequest,
        context: &RequestContext,
    ) -> Result<(), Status> {
        let metrics = OtlpMetricsConverter::convert(request, context);
        if metrics.is_empty() {
            return Ok(());
        }
        self.writer
            .insert_metrics(&metrics)
            .await
            .map_err(|e| Status::internal(format!("Failed to insert metrics: {}", e)))?;

        tracing::info!(
            tenant_id = %context.tenant_id,
            project_id = %context.project_id,
            metrics = metrics.len(),
            "Directly persisted metrics"
        );
        Ok(())
    }
}
