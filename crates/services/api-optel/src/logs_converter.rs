//! OTLP logs protobuf to internal model converter

use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord as OtlpLogRecord;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use uuid::Uuid;
use zradar_models::{LogRecord, RequestContext};

/// Converts an `ExportLogsServiceRequest` into a flat `Vec<LogRecord>`.
pub struct OtlpLogsConverter;

impl OtlpLogsConverter {
    /// Convert a full logs export request into internal `LogRecord` records.
    pub fn convert(
        request: ExportLogsServiceRequest,
        context: &RequestContext,
    ) -> Vec<LogRecord> {
        let mut out = Vec::new();

        for resource_logs in request.resource_logs {
            let resource = resource_logs.resource.as_ref();
            let resource_attrs = resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);

            let service_name = extract_string_attr(resource_attrs, "service.name")
                .unwrap_or_else(|| "unknown".to_string());
            let agent_name =
                extract_string_attr(resource_attrs, "agent.name").unwrap_or_default();
            let resource_json = attrs_to_json(resource_attrs);

            for scope_logs in resource_logs.scope_logs {
                for log_record in scope_logs.log_records {
                    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    let record = convert_log_record(
                        log_record,
                        &service_name,
                        &agent_name,
                        &resource_json,
                        now,
                        context,
                    );
                    out.push(record);
                }
            }
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn convert_log_record(
    log: OtlpLogRecord,
    service_name: &str,
    agent_name: &str,
    resource_json: &str,
    now_ns: i64,
    context: &RequestContext,
) -> LogRecord {
    let timestamp = if log.time_unix_nano > 0 {
        log.time_unix_nano as i64
    } else {
        now_ns
    };

    let severity = if !log.severity_text.is_empty() {
        log.severity_text.clone()
    } else {
        severity_number_to_text(log.severity_number)
    };

    let message = log
        .body
        .as_ref()
        .and_then(|b| b.value.as_ref())
        .map(|v| match v {
            AnyValue::StringValue(s) => s.clone(),
            AnyValue::IntValue(i) => i.to_string(),
            AnyValue::DoubleValue(d) => d.to_string(),
            AnyValue::BoolValue(b) => b.to_string(),
            _ => String::new(),
        })
        .unwrap_or_default();

    let trace_id = if !log.trace_id.is_empty() {
        hex::encode(&log.trace_id)
    } else {
        String::new()
    };
    let span_id = if !log.span_id.is_empty() {
        hex::encode(&log.span_id)
    } else {
        String::new()
    };

    let user_id =
        extract_string_attr(&log.attributes, "user.id").unwrap_or_default();
    let session_id =
        extract_string_attr(&log.attributes, "session.id").unwrap_or_default();
    let attributes_json = attrs_to_json(&log.attributes);

    LogRecord {
        id: Uuid::new_v4().to_string(),
        timestamp,
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        trace_id,
        span_id,
        severity,
        service_name: service_name.to_string(),
        message,
        attributes: attributes_json,
        resource: resource_json.to_string(),
        agent_name: agent_name.to_string(),
        session_id,
        user_id,
        created_at: now_ns,
    }
}

/// Map a `SeverityNumber` integer to a text string.
/// OTLP spec: 1-4=TRACE, 5-8=DEBUG, 9-12=INFO, 13-16=WARN, 17-20=ERROR, 21-24=FATAL
fn severity_number_to_text(severity_number: i32) -> String {
    match severity_number {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=20 => "ERROR",
        21..=24 => "FATAL",
        _ => "INFO",
    }
    .to_string()
}

/// Extract a string attribute by key from a list of `KeyValue` pairs.
fn extract_string_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find(|kv| kv.key == key).and_then(|kv| {
        kv.value.as_ref().and_then(|v| match &v.value {
            Some(AnyValue::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
    })
}

/// Serialize a list of `KeyValue` pairs to a JSON object string.
fn attrs_to_json(attrs: &[KeyValue]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .map(|kv| {
            let v = kv
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .map(|v| match v {
                    AnyValue::StringValue(s) => serde_json::Value::String(s.clone()),
                    AnyValue::IntValue(i) => serde_json::Value::Number((*i).into()),
                    AnyValue::DoubleValue(d) => {
                        serde_json::Number::from_f64(*d)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    AnyValue::BoolValue(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::Null,
                })
                .unwrap_or(serde_json::Value::Null);
            (kv.key.clone(), v)
        })
        .collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_number_info() {
        // SeverityNumber::Info = 9
        assert_eq!(severity_number_to_text(9), "INFO");
    }

    #[test]
    fn test_severity_number_error() {
        // SeverityNumber::Error = 17
        assert_eq!(severity_number_to_text(17), "ERROR");
    }

    #[test]
    fn test_severity_number_unknown() {
        assert_eq!(severity_number_to_text(0), "INFO");
    }
}
