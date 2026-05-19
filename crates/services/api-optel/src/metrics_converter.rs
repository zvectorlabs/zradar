//! OTLP metrics protobuf to internal model converter

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{
    HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
};
use zradar_models::{Metric, RequestContext};

/// Converts an `ExportMetricsServiceRequest` into a flat `Vec<Metric>`.
pub struct OtlpMetricsConverter;

impl OtlpMetricsConverter {
    /// Convert a full metrics export request into internal `Metric` records.
    pub fn convert(request: ExportMetricsServiceRequest, context: &RequestContext) -> Vec<Metric> {
        let mut out = Vec::new();

        for resource_metrics in request.resource_metrics {
            let resource = resource_metrics.resource.as_ref();
            let service_name = extract_string_attr(
                resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]),
                "service.name",
            )
            .unwrap_or_else(|| "unknown".to_string());
            let agent_name = extract_string_attr(
                resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]),
                "agent.name",
            )
            .unwrap_or_default();
            let resource_json =
                attrs_to_json(resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]));

            for scope_metrics in resource_metrics.scope_metrics {
                for metric in scope_metrics.metrics {
                    let metric_name = metric.name.clone();

                    match metric.data {
                        Some(Data::Gauge(g)) => {
                            for dp in g.data_points {
                                out.push(number_dp_to_metric(
                                    &metric_name,
                                    "GAUGE",
                                    dp,
                                    &service_name,
                                    &agent_name,
                                    &resource_json,
                                    context,
                                ));
                            }
                        }
                        Some(Data::Sum(s)) => {
                            for dp in s.data_points {
                                out.push(number_dp_to_metric(
                                    &metric_name,
                                    "COUNTER",
                                    dp,
                                    &service_name,
                                    &agent_name,
                                    &resource_json,
                                    context,
                                ));
                            }
                        }
                        Some(Data::Histogram(h)) => {
                            for dp in h.data_points {
                                out.push(histogram_dp_to_metric(
                                    &metric_name,
                                    dp,
                                    &service_name,
                                    &agent_name,
                                    context,
                                ));
                            }
                        }
                        Some(Data::Summary(s)) => {
                            for dp in s.data_points {
                                out.push(summary_dp_to_metric(
                                    &metric_name,
                                    dp,
                                    &service_name,
                                    &agent_name,
                                    context,
                                ));
                            }
                        }
                        Some(Data::ExponentialHistogram(eh)) => {
                            // Treat each exponential histogram point as a histogram
                            for dp in eh.data_points {
                                out.push(Metric {
                                    metric_name: metric_name.clone(),
                                    metric_type: "HISTOGRAM".to_string(),
                                    timestamp: dp.time_unix_nano as i64,
                                    tenant_id: context.tenant_id.clone(),
                                    project_id: context.project_id.clone(),
                                    value: dp.sum.unwrap_or(0.0),
                                    count: dp.count as i64,
                                    sum: dp.sum.unwrap_or(0.0),
                                    min: dp.min.unwrap_or(0.0),
                                    max: dp.max.unwrap_or(0.0),
                                    service_name: service_name.clone(),
                                    agent_name: agent_name.clone(),
                                    user_id: extract_string_attr(&dp.attributes, "user.id")
                                        .unwrap_or_default(),
                                    session_id: extract_string_attr(&dp.attributes, "session.id")
                                        .unwrap_or_default(),
                                    labels: attrs_to_json(&dp.attributes),
                                });
                            }
                        }
                        None => {}
                    }
                }
            }
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn number_dp_to_metric(
    metric_name: &str,
    metric_type: &str,
    dp: NumberDataPoint,
    service_name: &str,
    agent_name: &str,
    _resource_json: &str,
    context: &RequestContext,
) -> Metric {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    let value = match dp.value {
        Some(Value::AsDouble(d)) => d,
        Some(Value::AsInt(i)) => i as f64,
        None => 0.0,
    };
    Metric {
        metric_name: metric_name.to_string(),
        metric_type: metric_type.to_string(),
        timestamp: dp.time_unix_nano as i64,
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        value,
        count: 1,
        sum: value,
        min: value,
        max: value,
        service_name: service_name.to_string(),
        agent_name: agent_name.to_string(),
        user_id: extract_string_attr(&dp.attributes, "user.id").unwrap_or_default(),
        session_id: extract_string_attr(&dp.attributes, "session.id").unwrap_or_default(),
        labels: attrs_to_json(&dp.attributes),
    }
}

fn histogram_dp_to_metric(
    metric_name: &str,
    dp: HistogramDataPoint,
    service_name: &str,
    agent_name: &str,
    context: &RequestContext,
) -> Metric {
    let sum = dp.sum.unwrap_or(0.0);
    Metric {
        metric_name: metric_name.to_string(),
        metric_type: "HISTOGRAM".to_string(),
        timestamp: dp.time_unix_nano as i64,
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        value: if dp.count > 0 {
            sum / dp.count as f64
        } else {
            0.0
        },
        count: dp.count as i64,
        sum,
        min: dp.min.unwrap_or(0.0),
        max: dp.max.unwrap_or(0.0),
        service_name: service_name.to_string(),
        agent_name: agent_name.to_string(),
        user_id: extract_string_attr(&dp.attributes, "user.id").unwrap_or_default(),
        session_id: extract_string_attr(&dp.attributes, "session.id").unwrap_or_default(),
        labels: attrs_to_json(&dp.attributes),
    }
}

fn summary_dp_to_metric(
    metric_name: &str,
    dp: SummaryDataPoint,
    service_name: &str,
    agent_name: &str,
    context: &RequestContext,
) -> Metric {
    Metric {
        metric_name: metric_name.to_string(),
        metric_type: "SUMMARY".to_string(),
        timestamp: dp.time_unix_nano as i64,
        tenant_id: context.tenant_id.clone(),
        project_id: context.project_id.clone(),
        value: if dp.count > 0 {
            dp.sum / dp.count as f64
        } else {
            0.0
        },
        count: dp.count as i64,
        sum: dp.sum,
        min: 0.0,
        max: 0.0,
        service_name: service_name.to_string(),
        agent_name: agent_name.to_string(),
        user_id: extract_string_attr(&dp.attributes, "user.id").unwrap_or_default(),
        session_id: extract_string_attr(&dp.attributes, "session.id").unwrap_or_default(),
        labels: attrs_to_json(&dp.attributes),
    }
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
                    AnyValue::DoubleValue(d) => serde_json::Number::from_f64(*d)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
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
    fn test_attrs_to_json_empty() {
        assert_eq!(attrs_to_json(&[]), "{}");
    }

    #[test]
    fn test_extract_string_attr_found() {
        use opentelemetry_proto::tonic::common::v1::AnyValue as ProtoAnyValue;
        let kv = KeyValue {
            key: "service.name".to_string(),
            value: Some(ProtoAnyValue {
                value: Some(AnyValue::StringValue("my-service".to_string())),
            }),
        };
        assert_eq!(
            extract_string_attr(&[kv], "service.name"),
            Some("my-service".to_string())
        );
    }

    #[test]
    fn test_extract_string_attr_not_found() {
        assert_eq!(extract_string_attr(&[], "service.name"), None);
    }
}
