//! OTLP parser/resource caps (OQ27).
//!
//! These caps reject oversized decoded OTLP requests before conversion so a
//! single payload cannot create unbounded memory or CPU work.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tonic::Status;

/// Maximum ResourceSpans/ResourceMetrics/ResourceLogs in one export request.
pub const MAX_RESOURCE_GROUPS_PER_REQUEST: usize = 1_024;
/// Maximum ScopeSpans/ScopeMetrics/ScopeLogs in one export request.
pub const MAX_SCOPE_GROUPS_PER_REQUEST: usize = 4_096;
/// Maximum records in one export request after expanding scopes.
pub const MAX_RECORDS_PER_REQUEST: usize = 10_000;

/// Parser cap violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserCapError {
    /// Too many ResourceSpans in a trace request.
    ResourceSpans,
    /// Too many ScopeSpans in a trace request.
    ScopeSpans,
    /// Too many spans in a trace request.
    Spans,
    /// Too many ResourceMetrics in a metrics request.
    ResourceMetrics,
    /// Too many ScopeMetrics in a metrics request.
    ScopeMetrics,
    /// Too many metrics in a metrics request.
    Metrics,
    /// Too many ResourceLogs in a logs request.
    ResourceLogs,
    /// Too many ScopeLogs in a logs request.
    ScopeLogs,
    /// Too many log records in a logs request.
    LogRecords,
}

impl ParserCapError {
    /// Stable machine-readable reason string.
    pub fn message(self) -> &'static str {
        match self {
            Self::ResourceSpans => "too_many_resource_spans",
            Self::ScopeSpans => "too_many_scope_spans",
            Self::Spans => "too_many_spans",
            Self::ResourceMetrics => "too_many_resource_metrics",
            Self::ScopeMetrics => "too_many_scope_metrics",
            Self::Metrics => "too_many_metrics",
            Self::ResourceLogs => "too_many_resource_logs",
            Self::ScopeLogs => "too_many_scope_logs",
            Self::LogRecords => "too_many_log_records",
        }
    }

    /// Convert to gRPC status.
    pub fn into_status(self) -> Status {
        Status::resource_exhausted(self.message())
    }
}

/// Validate trace request caps.
pub fn validate_trace_request(req: &ExportTraceServiceRequest) -> Result<(), ParserCapError> {
    if req.resource_spans.len() > MAX_RESOURCE_GROUPS_PER_REQUEST {
        return Err(ParserCapError::ResourceSpans);
    }

    let mut scopes = 0usize;
    let mut spans = 0usize;
    for resource_spans in &req.resource_spans {
        scopes = scopes.saturating_add(resource_spans.scope_spans.len());
        if scopes > MAX_SCOPE_GROUPS_PER_REQUEST {
            return Err(ParserCapError::ScopeSpans);
        }
        for scope_spans in &resource_spans.scope_spans {
            spans = spans.saturating_add(scope_spans.spans.len());
            if spans > MAX_RECORDS_PER_REQUEST {
                return Err(ParserCapError::Spans);
            }
        }
    }

    Ok(())
}

/// Validate metrics request caps.
pub fn validate_metrics_request(req: &ExportMetricsServiceRequest) -> Result<(), ParserCapError> {
    if req.resource_metrics.len() > MAX_RESOURCE_GROUPS_PER_REQUEST {
        return Err(ParserCapError::ResourceMetrics);
    }

    let mut scopes = 0usize;
    let mut metrics = 0usize;
    for resource_metrics in &req.resource_metrics {
        scopes = scopes.saturating_add(resource_metrics.scope_metrics.len());
        if scopes > MAX_SCOPE_GROUPS_PER_REQUEST {
            return Err(ParserCapError::ScopeMetrics);
        }
        for scope_metrics in &resource_metrics.scope_metrics {
            metrics = metrics.saturating_add(scope_metrics.metrics.len());
            if metrics > MAX_RECORDS_PER_REQUEST {
                return Err(ParserCapError::Metrics);
            }
        }
    }

    Ok(())
}

/// Validate logs request caps.
pub fn validate_logs_request(req: &ExportLogsServiceRequest) -> Result<(), ParserCapError> {
    if req.resource_logs.len() > MAX_RESOURCE_GROUPS_PER_REQUEST {
        return Err(ParserCapError::ResourceLogs);
    }

    let mut scopes = 0usize;
    let mut logs = 0usize;
    for resource_logs in &req.resource_logs {
        scopes = scopes.saturating_add(resource_logs.scope_logs.len());
        if scopes > MAX_SCOPE_GROUPS_PER_REQUEST {
            return Err(ParserCapError::ScopeLogs);
        }
        for scope_logs in &resource_logs.scope_logs {
            logs = logs.saturating_add(scope_logs.log_records.len());
            if logs > MAX_RECORDS_PER_REQUEST {
                return Err(ParserCapError::LogRecords);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    #[test]
    fn test_trace_caps_reject_too_many_spans() {
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default(); MAX_RECORDS_PER_REQUEST + 1],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_eq!(
            validate_trace_request(&req).unwrap_err().message(),
            "too_many_spans"
        );
    }

    #[test]
    fn test_metrics_caps_reject_too_many_metrics() {
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric::default(); MAX_RECORDS_PER_REQUEST + 1],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_eq!(
            validate_metrics_request(&req).unwrap_err().message(),
            "too_many_metrics"
        );
    }

    #[test]
    fn test_logs_caps_reject_too_many_records() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default(); MAX_RECORDS_PER_REQUEST + 1],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_eq!(
            validate_logs_request(&req).unwrap_err().message(),
            "too_many_log_records"
        );
    }
}
