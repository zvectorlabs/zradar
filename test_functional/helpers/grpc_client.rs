#![allow(clippy::result_large_err)]
//! gRPC client for testing OTLP endpoints

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord as OtlpLogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

/// Type alias for span definition: (name, span_id, parent_span_id)
type SpanDef<'a> = (&'a str, &'a [u8; 8], Option<&'a [u8; 8]>);

/// Extended span definition with per-span attributes and status
pub struct SpanDefExt {
    pub name: String,
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub attributes: Vec<(String, AnyValue)>,
    pub status_code: Option<i32>, // 0=UNSET, 1=OK, 2=ERROR
}

/// gRPC client for OTLP telemetry ingestion
pub struct OtlpClient {
    grpc_url: String,
    api_key: Option<String>,
    workspace_id: Option<String>,
}

impl OtlpClient {
    /// Create a new OTLP client
    pub fn new(grpc_url: String) -> Self {
        Self {
            grpc_url,
            api_key: None,
            workspace_id: None,
        }
    }

    /// Set API key for authentication
    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.api_key = Some(api_key);
        self
    }

    /// Set API key for authentication (mutable)
    pub fn set_api_key(&mut self, api_key: String) {
        self.api_key = Some(api_key);
    }

    /// Set workspace ID header override
    pub fn with_workspace_id(mut self, workspace_id: String) -> Self {
        self.workspace_id = Some(workspace_id);
        self
    }

    // ========================================================================
    // Trace Export
    // ========================================================================

    /// Send traces via OTLP/gRPC
    pub async fn export_traces(&self, request: ExportTraceServiceRequest) -> Result<()> {
        let channel = Channel::from_shared(self.grpc_url.clone())
            .context("Invalid gRPC URL")?
            .connect()
            .await
            .context("Failed to connect to gRPC server")?;

        // Always use an interceptor to have consistent type
        let api_key_token = self
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {}", key)).ok());
        let workspace_id_val = self
            .workspace_id
            .as_ref()
            .and_then(|v| MetadataValue::try_from(v.as_str()).ok());

        let mut client =
            TraceServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(val) = &workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", val.clone());
                }
                Ok(req)
            });

        client
            .export(request)
            .await
            .context("Failed to export traces")?;

        Ok(())
    }

    /// Send a simple test trace
    pub async fn send_test_trace(
        &self,
        service_name: &str,
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        span_name: &str,
    ) -> Result<()> {
        let request = self.build_test_trace(service_name, trace_id, span_id, span_name);
        self.export_traces(request).await
    }

    /// Build a test trace request
    pub fn build_test_trace(
        &self,
        service_name: &str,
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        span_name: &str,
    ) -> ExportTraceServiceRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let span = Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            parent_span_id: vec![],
            name: span_name.to_string(),
            kind: 1,                                   // SPAN_KIND_INTERNAL
            start_time_unix_nano: now - 1_000_000_000, // 1 second ago
            end_time_unix_nano: now,
            attributes: vec![
                KeyValue {
                    key: "http.method".to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "GET".to_string(),
                            ),
                        ),
                    }),
                    ..Default::default()
                },
                KeyValue {
                    key: "http.status_code".to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(200),
                        ),
                    }),
                    ..Default::default()
                },
            ],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 0, // STATUS_CODE_UNSET
            }),
            ..Default::default()
        };

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            spans: vec![span],
            schema_url: String::new(),
        };

        let resource_spans = ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope_spans],
            schema_url: String::new(),
        };

        ExportTraceServiceRequest {
            resource_spans: vec![resource_spans],
        }
    }

    /// Build a test trace with explicit start/end timestamps (nanoseconds since epoch).
    ///
    /// Useful for retention tests where you need to inject data with known
    /// timestamps in the past.
    pub fn build_test_trace_with_timestamp(
        &self,
        service_name: &str,
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        span_name: &str,
        start_ns: u64,
        end_ns: u64,
    ) -> ExportTraceServiceRequest {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let span = Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            parent_span_id: vec![],
            name: span_name.to_string(),
            kind: 1,
            start_time_unix_nano: start_ns,
            end_time_unix_nano: end_ns,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 0,
            }),
            ..Default::default()
        };

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            spans: vec![span],
            schema_url: String::new(),
        };

        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![scope_spans],
                schema_url: String::new(),
            }],
        }
    }

    // ========================================================================
    // Metrics Export
    // ========================================================================

    /// Send metrics via OTLP/gRPC
    pub async fn export_metrics(&self, request: ExportMetricsServiceRequest) -> Result<()> {
        let channel = Channel::from_shared(self.grpc_url.clone())
            .context("Invalid gRPC URL")?
            .connect()
            .await
            .context("Failed to connect to gRPC server")?;

        let api_key_token = self
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {}", key)).ok());
        let _workspace_id_val = self
            .workspace_id
            .as_ref()
            .and_then(|v| MetadataValue::try_from(v.as_str()).ok());
        let _workspace_id_val = self
            .workspace_id
            .as_ref()
            .and_then(|v| MetadataValue::try_from(v.as_str()).ok());

        let mut client =
            MetricsServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(val) = &_workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", val.clone());
                }
                if let Some(val) = &_workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", val.clone());
                }
                Ok(req)
            });

        client
            .export(request)
            .await
            .context("Failed to export metrics")?;

        Ok(())
    }

    /// Build a gauge metric request
    pub fn build_gauge_metric(
        &self,
        service_name: &str,
        metric_name: &str,
        value: f64,
    ) -> ExportMetricsServiceRequest {
        self.build_number_metric_request(service_name, metric_name, value, false)
    }

    /// Build a counter (cumulative sum) metric request
    pub fn build_counter_metric(
        &self,
        service_name: &str,
        metric_name: &str,
        value: f64,
    ) -> ExportMetricsServiceRequest {
        self.build_number_metric_request(service_name, metric_name, value, true)
    }

    fn build_number_metric_request(
        &self,
        service_name: &str,
        metric_name: &str,
        value: f64,
        is_counter: bool,
    ) -> ExportMetricsServiceRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let data_point = NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: now - 60_000_000_000,
            time_unix_nano: now,
            value: Some(number_data_point::Value::AsDouble(value)),
            exemplars: vec![],
            flags: 0,
        };

        let data = if is_counter {
            Data::Sum(Sum {
                data_points: vec![data_point],
                aggregation_temporality: 2, // CUMULATIVE
                is_monotonic: true,
            })
        } else {
            Data::Gauge(Gauge {
                data_points: vec![data_point],
            })
        };

        let metric = Metric {
            name: metric_name.to_string(),
            description: String::new(),
            unit: String::new(),
            data: Some(data),
            ..Default::default()
        };

        let scope_metrics = ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            metrics: vec![metric],
            schema_url: String::new(),
        };

        let resource_metrics = ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![scope_metrics],
            schema_url: String::new(),
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![resource_metrics],
        }
    }

    // ========================================================================
    // Logs Export
    // ========================================================================

    /// Send logs via OTLP/gRPC
    pub async fn export_logs(&self, request: ExportLogsServiceRequest) -> Result<()> {
        let channel = Channel::from_shared(self.grpc_url.clone())
            .context("Invalid gRPC URL")?
            .connect()
            .await
            .context("Failed to connect to gRPC server")?;

        let api_key_token = self
            .api_key
            .as_ref()
            .and_then(|key| MetadataValue::try_from(format!("Bearer {}", key)).ok());
        let _workspace_id_val = self
            .workspace_id
            .as_ref()
            .and_then(|v| MetadataValue::try_from(v.as_str()).ok());
        let _workspace_id_val = self
            .workspace_id
            .as_ref()
            .and_then(|v| MetadataValue::try_from(v.as_str()).ok());

        let mut client =
            LogsServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                if let Some(val) = &_workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", val.clone());
                }
                if let Some(val) = &_workspace_id_val {
                    req.metadata_mut().insert("x-workspace-id", val.clone());
                }
                Ok(req)
            });

        client
            .export(request)
            .await
            .context("Failed to export logs")?;

        Ok(())
    }

    /// Build a simple log export request
    pub fn build_log_request(
        &self,
        service_name: &str,
        severity_number: i32,
        message: &str,
    ) -> ExportLogsServiceRequest {
        self.build_log_request_with_attrs(service_name, severity_number, message, &[], &[], &[])
    }

    /// Build a log export request with trace context and attributes
    pub fn build_log_request_with_attrs(
        &self,
        service_name: &str,
        severity_number: i32,
        message: &str,
        trace_id: &[u8],
        span_id: &[u8],
        attributes: &[(&str, &str)],
    ) -> ExportLogsServiceRequest {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyVal;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let severity_text = match severity_number {
            1..=4 => "TRACE",
            5..=8 => "DEBUG",
            9..=12 => "INFO",
            13..=16 => "WARN",
            17..=20 => "ERROR",
            21..=24 => "FATAL",
            _ => "INFO",
        };

        let log_attrs: Vec<KeyValue> = attributes
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: Some(AnyValue {
                    value: Some(AnyVal::StringValue(v.to_string())),
                }),
                ..Default::default()
            })
            .collect();

        let log_record = OtlpLogRecord {
            time_unix_nano: now,
            observed_time_unix_nano: now,
            severity_number,
            severity_text: severity_text.to_string(),
            body: Some(AnyValue {
                value: Some(AnyVal::StringValue(message.to_string())),
            }),
            attributes: log_attrs,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyVal::StringValue(service_name.to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let scope_logs = ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            log_records: vec![log_record],
            schema_url: String::new(),
        };

        let resource_logs = ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![scope_logs],
            schema_url: String::new(),
        };

        ExportLogsServiceRequest {
            resource_logs: vec![resource_logs],
        }
    }

    /// Build a complex multi-span trace
    pub fn build_multi_span_trace(
        &self,
        service_name: &str,
        trace_id: &[u8; 16],
        spans: Vec<SpanDef>, // (name, span_id, parent_span_id)
    ) -> ExportTraceServiceRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let built_spans: Vec<Span> = spans
            .iter()
            .enumerate()
            .map(|(i, (name, span_id, parent_span_id))| {
                let start_offset = (i as u64) * 100_000_000; // 100ms between spans

                Span {
                    trace_id: trace_id.to_vec(),
                    span_id: span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: parent_span_id.map(|p| p.to_vec()).unwrap_or_default(),
                    name: name.to_string(),
                    kind: 1,
                    start_time_unix_nano: now - 1_000_000_000 + start_offset,
                    end_time_unix_nano: now - 900_000_000 + start_offset,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 0,
                    }),
                    ..Default::default()
                }
            })
            .collect();

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            spans: built_spans,
            schema_url: String::new(),
        };

        let resource_spans = ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope_spans],
            schema_url: String::new(),
        };

        ExportTraceServiceRequest {
            resource_spans: vec![resource_spans],
        }
    }

    /// Build a multi-span trace with per-span attributes and status codes.
    ///
    /// Unlike `build_multi_span_trace`, each span carries its own set of OTLP
    /// attributes and an optional status code, making it suitable for building
    /// realistic agent workflow trees (AGENT → TOOL, GENERATION, sub-AGENT, …).
    pub fn build_multi_span_trace_with_attributes(
        &self,
        service_name: &str,
        trace_id: &[u8; 16],
        spans: Vec<SpanDefExt>,
    ) -> ExportTraceServiceRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let built_spans: Vec<Span> = spans
            .into_iter()
            .enumerate()
            .map(|(i, def)| {
                let start_offset = (i as u64) * 10_000_000; // 10ms between spans

                let attributes: Vec<KeyValue> = def
                    .attributes
                    .into_iter()
                    .map(|(key, value)| KeyValue {
                        key,
                        value: Some(value),
                        ..Default::default()
                    })
                    .collect();

                Span {
                    trace_id: trace_id.to_vec(),
                    span_id: def.span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: def.parent_span_id.map(|p| p.to_vec()).unwrap_or_default(),
                    name: def.name,
                    kind: 1, // SPAN_KIND_INTERNAL
                    start_time_unix_nano: now - 2_000_000_000 + start_offset,
                    end_time_unix_nano: now - 1_000_000_000 + start_offset,
                    attributes,
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: def.status_code.unwrap_or(0),
                    }),
                    ..Default::default()
                }
            })
            .collect();

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "test-instrumentation".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            spans: built_spans,
            schema_url: String::new(),
        };

        let resource_spans = ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope_spans],
            schema_url: String::new(),
        };

        ExportTraceServiceRequest {
            resource_spans: vec![resource_spans],
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate a random trace ID
pub fn random_trace_id() -> [u8; 16] {
    use rand::RngExt;
    rand::rng().random()
}

/// Generate a random span ID
pub fn random_span_id() -> [u8; 8] {
    use rand::RngExt;
    rand::rng().random()
}

/// Convert hex string to trace ID
pub fn hex_to_trace_id(hex: &str) -> Result<[u8; 16]> {
    let bytes = hex::decode(hex).context("Invalid hex string")?;
    if bytes.len() != 16 {
        anyhow::bail!("Trace ID must be 16 bytes, got {}", bytes.len());
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(&bytes);
    Ok(array)
}

/// Convert hex string to span ID
pub fn hex_to_span_id(hex: &str) -> Result<[u8; 8]> {
    let bytes = hex::decode(hex).context("Invalid hex string")?;
    if bytes.len() != 8 {
        anyhow::bail!("Span ID must be 8 bytes, got {}", bytes.len());
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes);
    Ok(array)
}

/// Convert trace ID to hex string
pub fn trace_id_to_hex(trace_id: &[u8; 16]) -> String {
    hex::encode(trace_id)
}

/// Convert span ID to hex string
pub fn span_id_to_hex(span_id: &[u8; 8]) -> String {
    hex::encode(span_id)
}
