//! Shared OTLP attribute serialization utilities (Phase 1 R1.9).
//!
//! Centralizes the `AnyValue → serde_json::Value` conversion that was
//! previously duplicated between `converter.rs` (handling `ArrayValue` and
//! `KvlistValue`) and `logs_converter.rs` (missing those variants).
//!
//! Both call sites now use [`any_value_to_json`] and [`attrs_to_json`] from
//! this module, ensuring consistent handling of all OTLP value kinds.

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

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
        // String table index — no table context in this helper; treat as absent.
        Some(Value::StringValueStrindex(_)) => serde_json::Value::Null,
        None => serde_json::Value::Null,
    }
}

/// Streams an OTLP [`AnyValue`] to JSON identically to [`any_value_to_json`] +
/// `serde_json::to_string`, but without materializing an intermediate
/// `serde_json::Value`. `serde_json` drives this impl, so the bytes are
/// identical — including sorted nested-object keys (serde_json's `Map` is a
/// `BTreeMap`; this build has no `preserve_order`) and non-finite floats
/// serialized as `null`.
struct AnyValueSer<'a>(&'a AnyValue);

impl Serialize for AnyValueSer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        match &self.0.value {
            Some(Value::StringValue(s)) => ser.serialize_str(s),
            Some(Value::BoolValue(b)) => ser.serialize_bool(*b),
            Some(Value::IntValue(i)) => ser.serialize_i64(*i),
            // serde_json serializes non-finite f64 as `null`, matching
            // `any_value_to_json`'s `Number::from_f64(..).unwrap_or(Null)`.
            Some(Value::DoubleValue(d)) => ser.serialize_f64(*d),
            Some(Value::ArrayValue(arr)) => {
                let mut seq = ser.serialize_seq(Some(arr.values.len()))?;
                for v in &arr.values {
                    seq.serialize_element(&AnyValueSer(v))?;
                }
                seq.end()
            }
            Some(Value::KvlistValue(kv)) => {
                // Mirror serde_json::Map (BTreeMap): sorted keys, last-wins.
                let sorted: BTreeMap<&str, &AnyValue> = kv
                    .values
                    .iter()
                    .filter_map(|item| item.value.as_ref().map(|v| (item.key.as_str(), v)))
                    .collect();
                let mut map = ser.serialize_map(Some(sorted.len()))?;
                for (k, v) in sorted {
                    map.serialize_entry(k, &AnyValueSer(v))?;
                }
                map.end()
            }
            Some(Value::BytesValue(b)) => ser.serialize_str(&hex::encode(b)),
            Some(Value::StringValueStrindex(_)) => ser.serialize_none(),
            None => ser.serialize_none(),
        }
    }
}

/// Serialize a `KeyValue` slice to a compact JSON object string, byte-identical
/// to building a `serde_json::Map` via [`any_value_to_json`] and `to_string` —
/// but without cloning keys or allocating intermediate `Value`s. `skip(key)`
/// returns `true` for attributes to omit (used to scrub prompt/completion
/// content when capture is disabled).
pub fn attrs_to_json_filtered(attrs: &[KeyValue], skip: impl Fn(&str) -> bool) -> String {
    struct AttrsSer<'a, F>(&'a [KeyValue], F);
    impl<F: Fn(&str) -> bool> Serialize for AttrsSer<'_, F> {
        fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
            // BTreeMap mirrors serde_json::Map ordering (sorted, last-wins).
            let sorted: BTreeMap<&str, &AnyValue> = self
                .0
                .iter()
                .filter(|kv| !(self.1)(&kv.key))
                .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.as_str(), v)))
                .collect();
            let mut map = ser.serialize_map(Some(sorted.len()))?;
            for (k, v) in sorted {
                map.serialize_entry(k, &AnyValueSer(v))?;
            }
            map.end()
        }
    }
    serde_json::to_string(&AttrsSer(attrs, skip)).unwrap_or_else(|_| "{}".to_string())
}

/// Serialize a `KeyValue` slice to a compact JSON object string.
///
/// Replaces the per-module `attrs_to_json` helpers that previously missed
/// `ArrayValue` and `KvlistValue` (logs_converter.rs, R1.9 fix).
pub fn attrs_to_json(attrs: &[KeyValue]) -> String {
    attrs_to_json_filtered(attrs, |_| false)
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

    fn kv(k: &str, v: Value) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(av(v)),
            ..Default::default()
        }
    }

    /// Pre-optimization reference: build a `serde_json::Map` via
    /// `any_value_to_json` and stringify. The streaming serializer must match
    /// this byte-for-byte.
    fn reference_json(attrs: &[KeyValue]) -> String {
        let map: serde_json::Map<String, serde_json::Value> = attrs
            .iter()
            .filter_map(|kv| {
                kv.value
                    .as_ref()
                    .map(|v| (kv.key.clone(), any_value_to_json(v)))
            })
            .collect();
        serde_json::to_string(&map).unwrap()
    }

    #[test]
    fn attrs_to_json_matches_map_reference_byte_for_byte() {
        let attrs = vec![
            kv("z.last", Value::StringValue("end".into())),
            kv(
                "a.first",
                Value::StringValue("start \"quoted\" \n newline /\u{1f}".into()),
            ),
            kv("m.int", Value::IntValue(-42)),
            kv("m.double", Value::DoubleValue(1.5)),
            kv("m.nan", Value::DoubleValue(f64::NAN)),
            kv("m.bool", Value::BoolValue(true)),
            kv("m.bytes", Value::BytesValue(vec![0xde, 0xad, 0xbe, 0xef])),
            kv(
                "m.array",
                Value::ArrayValue(ArrayValue {
                    values: vec![av(Value::StringValue("x".into())), av(Value::IntValue(2))],
                }),
            ),
            // Nested kvlist with out-of-order keys — must sort like serde_json::Map.
            kv(
                "m.nested",
                Value::KvlistValue(KeyValueList {
                    values: vec![
                        KeyValue {
                            key: "b".into(),
                            value: Some(av(Value::IntValue(2))),
                            ..Default::default()
                        },
                        KeyValue {
                            key: "a".into(),
                            value: Some(av(Value::IntValue(1))),
                            ..Default::default()
                        },
                    ],
                }),
            ),
            // Duplicate key: last value wins (BTreeMap semantics).
            kv("dup", Value::IntValue(1)),
            kv("dup", Value::IntValue(2)),
            // Null inner value: dropped, matching the old map build.
            KeyValue {
                key: "skip.null".into(),
                value: None,
                ..Default::default()
            },
        ];
        assert_eq!(attrs_to_json(&attrs), reference_json(&attrs));
    }

    #[test]
    fn attrs_to_json_empty_is_empty_object() {
        assert_eq!(attrs_to_json(&[]), "{}");
    }

    #[test]
    fn attrs_to_json_filtered_skips_keys_and_matches_reference() {
        let attrs = vec![
            kv("keep", Value::StringValue("ok".into())),
            kv("gen_ai.content.prompt", Value::StringValue("secret".into())),
            kv("llm.input", Value::StringValue("secret".into())),
        ];
        let got = attrs_to_json_filtered(&attrs, |k| {
            k.starts_with("gen_ai.content.") || k == "llm.input"
        });
        let kept: Vec<KeyValue> = attrs
            .iter()
            .filter(|kv| !(kv.key.starts_with("gen_ai.content.") || kv.key == "llm.input"))
            .cloned()
            .collect();
        assert_eq!(got, reference_json(&kept));
        assert!(!got.contains("secret"));
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
                ..Default::default()
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
                ..Default::default()
            },
            KeyValue {
                key: "bool".to_string(),
                value: Some(av(Value::BoolValue(true))),
                ..Default::default()
            },
            KeyValue {
                key: "arr".to_string(),
                value: Some(av(Value::ArrayValue(ArrayValue {
                    values: vec![av(Value::IntValue(1)), av(Value::IntValue(2))],
                }))),
                ..Default::default()
            },
        ];
        let result = attrs_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["str"], "hello");
        assert_eq!(parsed["bool"], true);
        assert_eq!(parsed["arr"], serde_json::json!([1, 2]));
    }
}
