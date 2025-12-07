//! OTLP protobuf to internal model converter
//!
//! This module converts OpenTelemetry Protocol (OTLP) protobuf messages
//! to zradar's internal Span model.

use anyhow::Result;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span as OtlpSpan};
use zradar_models::{RequestContext, Span};

/// Converter for OTLP protobuf to internal models
pub struct OtlpConverter;

impl OtlpConverter {
    /// Convert OTLP ResourceSpans to internal Span format
    pub fn convert_resource_spans(
        resource_spans: ResourceSpans,
        context: &RequestContext,
    ) -> Result<Vec<Span>> {
        let mut spans = Vec::new();

        // Extract service name from resource attributes
        let service_name = resource_spans
            .resource
            .as_ref()
            .and_then(|r| r.attributes.iter().find(|attr| attr.key == "service.name"))
            .and_then(|attr| attr.value.as_ref())
            .and_then(|v| match &v.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    Some(s)
                }
                _ => None,
            })
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Process each scope (instrumentation library)
        for scope_spans in resource_spans.scope_spans {
            // Process each span
            for otlp_span in scope_spans.spans {
                let span = Self::convert_span(otlp_span, &service_name, context)?;
                spans.push(span);
            }
        }

        Ok(spans)
    }

    /// Convert a single OTLP Span to internal format
    fn convert_span(
        otlp_span: OtlpSpan,
        service_name: &str,
        context: &RequestContext,
    ) -> Result<Span> {
        use chrono::Utc;

        // Convert trace_id and span_id from bytes to hex string
        let trace_id = hex::encode(&otlp_span.trace_id);
        let span_id = hex::encode(&otlp_span.span_id);
        let parent_span_id = if !otlp_span.parent_span_id.is_empty() {
            hex::encode(&otlp_span.parent_span_id)
        } else {
            String::new()
        };

        // Extract timestamps (nanoseconds since epoch)
        let timestamp = otlp_span.start_time_unix_nano as i64;
        let duration_ns = (otlp_span.end_time_unix_nano - otlp_span.start_time_unix_nano) as i64;

        // Initialize span with basic fields
        let mut span_data = Span {
            trace_id,
            span_id,
            parent_span_id,
            timestamp,
            duration_ns,
            tenant_id: context.tenant_id.clone(),
            project_id: context.project_id.clone(),
            service_name: service_name.to_string(),
            span_name: otlp_span.name.clone(),
            ..Default::default()
        };

        // Extract attributes into a map for flexible storage
        let mut attributes_map = serde_json::Map::new();

        for attr in &otlp_span.attributes {
            if let Some(value) = &attr.value {
                let key = &attr.key;
                let val = Self::extract_attribute_value(value);

                // Map known attributes to span fields
                Self::map_attribute_to_span(&mut span_data, key, &val);

                // Store all attributes in JSON
                attributes_map.insert(key.clone(), val);
            }
        }

        // Convert serde_json::Map to HashMap for type detection
        use std::collections::HashMap;
        let attributes_hashmap: HashMap<String, serde_json::Value> = attributes_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Detect span type from attributes
        span_data.span_type = crate::SpanTypeMapper::detect_type(&attributes_hashmap, duration_ns);

        // Determine span kind
        span_data.span_kind = match otlp_span.kind {
            1 => "INTERNAL",
            2 => "SERVER",
            3 => "CLIENT",
            4 => "PRODUCER",
            5 => "CONSUMER",
            _ => "UNSPECIFIED",
        }
        .to_string();

        // Extract status
        if let Some(status) = otlp_span.status {
            span_data.status_code = match status.code {
                0 => "UNSET",
                1 => "OK",
                2 => "ERROR",
                _ => "UNSET",
            }
            .to_string();
            span_data.status_message = status.message;
        }

        // Store all attributes as JSON
        span_data.attributes = serde_json::to_string(&attributes_map)?;

        // Set timestamps
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        span_data.created_at = now;
        span_data.updated_at = now;

        Ok(span_data)
    }

    /// Map known OTLP attributes to Span fields
    fn map_attribute_to_span(span: &mut Span, key: &str, value: &serde_json::Value) {
        match key {
            // Agent context
            "invocation_id" | "gcp.vertex.agent.invocation_id" => {
                span.invocation_id = value.as_str().unwrap_or("").to_string();
            }
            "session_id" | "gcp.vertex.agent.session_id" => {
                span.session_id = value.as_str().unwrap_or("").to_string();
            }
            "user_id" => {
                span.user_id = value.as_str().unwrap_or("").to_string();
            }
            "agent.name" => {
                span.agent_name = value.as_str().unwrap_or("").to_string();
            }
            "agent.type" => {
                span.agent_type = value.as_str().unwrap_or("").to_string();
            }

            // LLM-specific
            "gen_ai.request.model" | "llm.model" => {
                span.llm_model = value.as_str().unwrap_or("").to_string();
            }
            "llm.input" | "gen_ai.prompt" => {
                span.llm_input = value.as_str().unwrap_or("").to_string();
            }
            "llm.output" | "gen_ai.completion" => {
                span.llm_output = value.as_str().unwrap_or("").to_string();
            }
            "llm.usage.prompt_tokens" | "gen_ai.usage.prompt_tokens" => {
                span.prompt_tokens = value.as_u64().unwrap_or(0) as i32;
            }
            "llm.usage.completion_tokens" | "gen_ai.usage.completion_tokens" => {
                span.completion_tokens = value.as_u64().unwrap_or(0) as i32;
            }
            "llm.usage.total_tokens" => {
                span.total_tokens = value.as_u64().unwrap_or(0) as i32;
            }
            "llm.cost.total_usd" => {
                span.total_cost_usd = value.as_f64().unwrap_or(0.0);
            }
            "llm.cost.prompt_usd" => {
                span.prompt_cost_usd = value.as_f64().unwrap_or(0.0);
            }
            "llm.cost.completion_usd" => {
                span.completion_cost_usd = value.as_f64().unwrap_or(0.0);
            }

            // Tool calls
            "gen_ai.tool.name" | "tool.name" => {
                span.tool_name = value.as_str().unwrap_or("").to_string();
            }
            "gen_ai.tool.call.id" | "tool.call.id" => {
                span.tool_call_id = value.as_str().unwrap_or("").to_string();
            }

            // Resource usage
            "resource.cpu.micros" => {
                span.resource_cpu_micros = value.as_i64().unwrap_or(0);
            }
            "resource.memory.bytes" => {
                span.resource_memory_bytes = value.as_i64().unwrap_or(0);
            }
            "resource.memory.peak" => {
                span.resource_memory_peak = value.as_i64().unwrap_or(0);
            }

            // Prompt management
            "prompt.id" => {
                span.prompt_id = value.as_str().unwrap_or("").to_string();
            }
            "prompt.name" => {
                span.prompt_name = value.as_str().unwrap_or("").to_string();
            }
            "prompt.version" => {
                span.prompt_version = value.as_u64().unwrap_or(0) as i32;
            }

            // Timing
            "time_to_first_token_ms" | "ttft_ms" => {
                span.time_to_first_token_ms = value.as_u64().unwrap_or(0) as i32;
            }

            // Versioning
            "agent.version" => {
                span.agent_version = value.as_str().unwrap_or("").to_string();
            }
            "sdk.version" => {
                span.sdk_version = value.as_str().unwrap_or("").to_string();
            }

            // Level
            "level" => {
                span.level = value.as_str().unwrap_or("INFO").to_string();
            }

            _ => {} // Unknown attribute, will be stored in attributes JSON
        }
    }

    /// Extract value from OTLP AnyValue
    fn extract_attribute_value(
        value: &opentelemetry_proto::tonic::common::v1::AnyValue,
    ) -> serde_json::Value {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        match &value.value {
            Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(Value::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(Value::IntValue(i)) => serde_json::Value::Number((*i).into()),
            Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Some(Value::ArrayValue(arr)) => {
                let values: Vec<_> = arr
                    .values
                    .iter()
                    .map(Self::extract_attribute_value)
                    .collect();
                serde_json::Value::Array(values)
            }
            Some(Value::KvlistValue(kv)) => {
                let mut map = serde_json::Map::new();
                for item in &kv.values {
                    if let Some(v) = &item.value {
                        map.insert(item.key.clone(), Self::extract_attribute_value(v));
                    }
                }
                serde_json::Value::Object(map)
            }
            Some(Value::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
            None => serde_json::Value::Null,
        }
    }
}
