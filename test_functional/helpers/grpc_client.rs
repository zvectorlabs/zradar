//! gRPC client for testing OTLP endpoints

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use tonic::transport::Channel;
use tonic::metadata::MetadataValue;
use std::time::{SystemTime, UNIX_EPOCH};

/// Type alias for span definition: (name, span_id, parent_span_id)
type SpanDef<'a> = (&'a str, &'a [u8; 8], Option<&'a [u8; 8]>);

/// gRPC client for OTLP telemetry ingestion
pub struct OtlpClient {
    grpc_url: String,
    api_key: Option<String>,
}

impl OtlpClient {
    /// Create a new OTLP client
    pub fn new(grpc_url: String) -> Self {
        Self {
            grpc_url,
            api_key: None,
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
        let api_key_token = self.api_key.as_ref().and_then(|key| {
            MetadataValue::try_from(format!("Bearer {}", key)).ok()
        });
        
        let mut client = TraceServiceClient::with_interceptor(
            channel,
            move |mut req: tonic::Request<()>| {
                if let Some(token) = &api_key_token {
                    req.metadata_mut().insert("authorization", token.clone());
                }
                Ok(req)
            },
        );
        
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
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        )),
                    }),
                },
            ],
            dropped_attributes_count: 0,
        };
        
        let span = Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            parent_span_id: vec![],
            name: span_name.to_string(),
            kind: 1, // SPAN_KIND_INTERNAL
            start_time_unix_nano: now - 1_000_000_000, // 1 second ago
            end_time_unix_nano: now,
            attributes: vec![
                KeyValue {
                    key: "http.method".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "GET".to_string(),
                        )),
                    }),
                },
                KeyValue {
                    key: "http.status_code".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(200)),
                    }),
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
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        )),
                    }),
                },
            ],
            dropped_attributes_count: 0,
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
    use rand::Rng;
    rand::thread_rng().r#gen()
}

/// Generate a random span ID
pub fn random_span_id() -> [u8; 8] {
    use rand::Rng;
    rand::thread_rng().r#gen()
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

