//! Shared OTLP attribute serialization utilities (Phase 1 R1.9).
//!
//! Centralizes the `AnyValue → serde_json::Value` conversion that was
//! previously duplicated between `converter.rs` (handling `ArrayValue` and
//! `KvlistValue`) and `logs_converter.rs` (missing those variants).
//!
//! Both call sites now use [`any_value_to_json`] and [`attrs_to_json`] from
//! this module, ensuring consistent handling of all OTLP value kinds.

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

/// Convert an OTLP [`AnyValue`] to a [`serde_json::Value`].
///
/// Handles all six OTLP value kinds:
/// `StringValue`, `IntValue`, `DoubleValue`, `BoolValue`, `ArrayValue`,
/// `KvlistValue`, and `BytesValue` (hex-encoded).
pub fn any_value_to_json(v: &AnyValue) -> serde_json::Value {
    match &v.value {
        Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Value::IntValue(i)) => serde_json::Value::Number((*i).into()),
        Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Value::ArrayValue(arr)) => {
            serde_json::Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(Value::KvlistValue(kv)) => {
            let mut map = serde_json::Map::new();
            for item in &kv.values {
                if let Some(val) = &item.value {
                    map.insert(item.key.clone(), any_value_to_json(val));
                }
            }
            serde_json::Value::Object(map)
        }
        Some(Value::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
        None => serde_json::Value::Null,
    }
}

/// Serialize a `KeyValue` slice to a compact JSON object string.
///
/// Replaces the per-module `attrs_to_json` helpers that previously missed
/// `ArrayValue` and `KvlistValue` (logs_converter.rs, R1.9 fix).
pub fn attrs_to_json(attrs: &[KeyValue]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .map(|v| (kv.key.clone(), any_value_to_json(v)))
        })
        .collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, KeyValueList, any_value::Value,
    };

    fn av(v: Value) -> AnyValue {
        AnyValue { value: Some(v) }
    }

    #[test]
    fn test_string_value() {
        let v = av(Value::StringValue("hello".to_string()));
        assert_eq!(
            any_value_to_json(&v),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_bool_value() {
        assert_eq!(
            any_value_to_json(&av(Value::BoolValue(true))),
            serde_json::Value::Bool(true)
        );
    }

    #[test]
    fn test_int_value() {
        let v = any_value_to_json(&av(Value::IntValue(42)));
        assert_eq!(v, serde_json::json!(42));
    }

    #[test]
    fn test_double_value() {
        let v = any_value_to_json(&av(Value::DoubleValue(1.23)));
        assert!((v.as_f64().unwrap() - 1.23).abs() < 1e-10);
    }

    #[test]
    fn test_array_value() {
        let v = av(Value::ArrayValue(ArrayValue {
            values: vec![
                av(Value::StringValue("a".to_string())),
                av(Value::IntValue(1)),
            ],
        }));
        let result = any_value_to_json(&v);
        assert_eq!(result, serde_json::json!(["a", 1]));
    }

    #[test]
    fn test_kvlist_value() {
        let v = av(Value::KvlistValue(KeyValueList {
            values: vec![KeyValue {
                key: "k".to_string(),
                value: Some(av(Value::StringValue("v".to_string()))),
            }],
        }));
        let result = any_value_to_json(&v);
        assert_eq!(result, serde_json::json!({"k": "v"}));
    }

    #[test]
    fn test_bytes_value_hex_encoded() {
        let v = av(Value::BytesValue(vec![0xde, 0xad]));
        assert_eq!(
            any_value_to_json(&v),
            serde_json::Value::String("dead".to_string())
        );
    }

    #[test]
    fn test_none_value() {
        let v = AnyValue { value: None };
        assert_eq!(any_value_to_json(&v), serde_json::Value::Null);
    }

    #[test]
    fn test_attrs_to_json_all_types() {
        let attrs = vec![
            KeyValue {
                key: "str".to_string(),
                value: Some(av(Value::StringValue("hello".to_string()))),
            },
            KeyValue {
                key: "bool".to_string(),
                value: Some(av(Value::BoolValue(true))),
            },
            KeyValue {
                key: "arr".to_string(),
                value: Some(av(Value::ArrayValue(ArrayValue {
                    values: vec![av(Value::IntValue(1)), av(Value::IntValue(2))],
                }))),
            },
        ];
        let result = attrs_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["str"], "hello");
        assert_eq!(parsed["bool"], true);
        assert_eq!(parsed["arr"], serde_json::json!([1, 2]));
    }
}
