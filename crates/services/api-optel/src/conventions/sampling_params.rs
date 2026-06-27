//! `gen_ai.request.*` sampling parameters → `Span.model_parameters` JSON (Phase 4 R4.4).
//!
//! Collects the standard OTel GenAI sampling-parameter attributes into the
//! `model_parameters` JSON column on the Span. The column is then surfaced
//! directly on `SpanDetail` so debugging "why did this LLM call generate so
//! verbosely?" doesn't require expanding the raw attributes blob.
//!
//! Explicit allowlist of keys to keep the column focused — anything outside
//! the list stays in the raw `attributes` JSON catch-all (no spillover).
//!
//! | OTel attribute                      | JSON key             | Type    |
//! |-------------------------------------|----------------------|---------|
//! | `gen_ai.request.temperature`        | `temperature`        | number  |
//! | `gen_ai.request.top_p`              | `top_p`              | number  |
//! | `gen_ai.request.top_k`              | `top_k`              | integer |
//! | `gen_ai.request.max_tokens`         | `max_tokens`         | integer |
//! | `gen_ai.request.frequency_penalty`  | `frequency_penalty`  | number  |
//! | `gen_ai.request.presence_penalty`   | `presence_penalty`   | number  |
//! | `gen_ai.request.stop_sequences`     | `stop_sequences`     | string (verbatim) |
//! | `gen_ai.request.seed`               | `seed`               | integer |

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Builds `span.model_parameters` from the `gen_ai.request.*` sampling allowlist.
pub struct SamplingParamsConvention;

impl AttributeConvention for SamplingParamsConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        let mut params = serde_json::Map::new();

        // f64 params — temperature / top_p / penalties.
        for key in [
            "gen_ai.request.temperature",
            "gen_ai.request.top_p",
            "gen_ai.request.frequency_penalty",
            "gen_ai.request.presence_penalty",
        ] {
            if let Some(v) = view.get_f64(key) {
                params.insert(json_key_for(key).to_string(), json_number(v));
                view.mark_consumed(key);
            }
        }

        // i64 params — top_k / max_tokens / seed.
        for key in [
            "gen_ai.request.top_k",
            "gen_ai.request.max_tokens",
            "gen_ai.request.seed",
        ] {
            if let Some(v) = view.get_i64(key) {
                params.insert(
                    json_key_for(key).to_string(),
                    serde_json::Value::Number(v.into()),
                );
                view.mark_consumed(key);
            }
        }

        // String param — stop_sequences (kept verbatim; producers send either
        // a single string or a comma/JSON list — we don't try to parse).
        if let Some(v) = view.get_str("gen_ai.request.stop_sequences") {
            params.insert(
                "stop_sequences".to_string(),
                serde_json::Value::String(v.to_string()),
            );
            view.mark_consumed("gen_ai.request.stop_sequences");
        }

        if !params.is_empty() {
            span.model_parameters = serde_json::to_string(&params).unwrap_or_else(|_| {
                tracing::warn!("failed to serialize model_parameters; defaulting to {{}}");
                "{}".to_string()
            });
        }
        // If nothing matched, leave the default "{}" untouched.
    }
}

/// Map the OTel-dotted attribute key to its JSON column key (strip the
/// `gen_ai.request.` prefix).
fn json_key_for(otel_key: &str) -> &str {
    otel_key.strip_prefix("gen_ai.request.").unwrap_or(otel_key)
}

/// Wrap an `f64` into a `serde_json::Value::Number`, with a sentinel for NaN /
/// Infinity which Number doesn't represent.
fn json_number(f: f64) -> serde_json::Value {
    serde_json::Number::from_f64(f)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    fn kv_f64(k: &str, v: f64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(v)),
            }),
        }
    }

    fn kv_i64(k: &str, v: i64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(v)),
            }),
        }
    }

    fn kv_str(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.to_string())),
            }),
        }
    }

    #[test]
    fn test_sampling_params_collects_all_allowlisted_keys() {
        let attrs = vec![
            kv_f64("gen_ai.request.temperature", 0.7),
            kv_f64("gen_ai.request.top_p", 0.95),
            kv_f64("gen_ai.request.frequency_penalty", 0.1),
            kv_f64("gen_ai.request.presence_penalty", -0.05),
            kv_i64("gen_ai.request.top_k", 40),
            kv_i64("gen_ai.request.max_tokens", 500),
            kv_i64("gen_ai.request.seed", 42),
            kv_str("gen_ai.request.stop_sequences", r#"["\n", "STOP"]"#),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        SamplingParamsConvention.apply(&view, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.model_parameters).unwrap();
        assert!((parsed["temperature"].as_f64().unwrap() - 0.7).abs() < 1e-9);
        assert!((parsed["top_p"].as_f64().unwrap() - 0.95).abs() < 1e-9);
        assert!((parsed["frequency_penalty"].as_f64().unwrap() - 0.1).abs() < 1e-9);
        assert!((parsed["presence_penalty"].as_f64().unwrap() - -0.05).abs() < 1e-9);
        assert_eq!(parsed["top_k"], 40);
        assert_eq!(parsed["max_tokens"], 500);
        assert_eq!(parsed["seed"], 42);
        assert_eq!(parsed["stop_sequences"], r#"["\n", "STOP"]"#);
    }

    #[test]
    fn test_sampling_params_no_keys_leaves_default() {
        let attrs = vec![kv_str("other.key", "ignored")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        SamplingParamsConvention.apply(&view, &mut span);
        assert_eq!(
            span.model_parameters, "{}",
            "absence of sampling keys must leave model_parameters at its default"
        );
    }

    #[test]
    fn test_sampling_params_partial_keys_only_records_present() {
        let attrs = vec![
            kv_f64("gen_ai.request.temperature", 1.0),
            kv_i64("gen_ai.request.max_tokens", 100),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        SamplingParamsConvention.apply(&view, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.model_parameters).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 2, "only the present keys should appear");
        assert!(obj.contains_key("temperature"));
        assert!(obj.contains_key("max_tokens"));
        assert!(!obj.contains_key("top_p"));
    }

    #[test]
    fn test_sampling_params_marks_keys_consumed() {
        let attrs = vec![
            kv_f64("gen_ai.request.temperature", 0.5),
            kv_i64("gen_ai.request.max_tokens", 256),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        SamplingParamsConvention.apply(&view, &mut span);
        assert!(view.is_consumed("gen_ai.request.temperature"));
        assert!(view.is_consumed("gen_ai.request.max_tokens"));
    }

    #[test]
    fn test_sampling_params_ignores_non_allowlisted_gen_ai_keys() {
        // gen_ai.request.model is intentionally NOT in the model_parameters
        // allowlist — it's routed to llm_model by other conventions.
        let attrs = vec![kv_str("gen_ai.request.model", "claude")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        SamplingParamsConvention.apply(&view, &mut span);
        assert_eq!(span.model_parameters, "{}");
    }
}
