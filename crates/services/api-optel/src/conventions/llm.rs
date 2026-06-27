//! `llm.*` attribute mappings (canonical LLM model / I/O / usage / cost).
//!
//! Owns: `llm.model`, `llm.input`, `llm.output`, `llm.usage.prompt_tokens`,
//! `llm.usage.completion_tokens`, `llm.usage.total_tokens`,
//! `llm.cost.total_usd`, `llm.cost.prompt_usd`, `llm.cost.completion_usd`.
//!
//! Legacy `gen_ai.*` aliases for model/input/output/usage live in
//! [`crate::conventions::gen_ai_legacy`] and run after this convention so
//! `gen_ai.*` overwrites `llm.*` when both are present — matching the
//! pre-refactor cascade arms (`gen_ai.request.model | llm.model`).

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps canonical `llm.*` attributes into `Span` fields.
pub struct LlmConvention;

impl AttributeConvention for LlmConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("llm.model") {
            span.llm_model = v.to_string();
            view.mark_consumed("llm.model");
        }
        if let Some(v) = view.get_str("llm.input") {
            span.llm_input = v.to_string();
            view.mark_consumed("llm.input");
        }
        if let Some(v) = view.get_str("llm.output") {
            span.llm_output = v.to_string();
            view.mark_consumed("llm.output");
        }
        if let Some(v) = view.get_u64("llm.usage.prompt_tokens") {
            span.prompt_tokens = v as i32;
            view.mark_consumed("llm.usage.prompt_tokens");
        }
        if let Some(v) = view.get_u64("llm.usage.completion_tokens") {
            span.completion_tokens = v as i32;
            view.mark_consumed("llm.usage.completion_tokens");
        }
        if let Some(v) = view.get_u64("llm.usage.total_tokens") {
            span.total_tokens = v as i32;
            view.mark_consumed("llm.usage.total_tokens");
        }
        if let Some(v) = view.get_f64("llm.cost.total_usd") {
            span.total_cost_usd = v;
            view.mark_consumed("llm.cost.total_usd");
        }
        if let Some(v) = view.get_f64("llm.cost.prompt_usd") {
            span.prompt_cost_usd = v;
            view.mark_consumed("llm.cost.prompt_usd");
        }
        if let Some(v) = view.get_f64("llm.cost.completion_usd") {
            span.completion_cost_usd = v;
            view.mark_consumed("llm.cost.completion_usd");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

    fn kv_str(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.to_string())),
            }),
        }
    }

    fn kv_int(k: &str, v: i64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(v)),
            }),
        }
    }

    fn kv_f64(k: &str, v: f64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(v)),
            }),
        }
    }

    #[test]
    fn test_llm_convention_populates_model_io_usage_cost() {
        let attrs = vec![
            kv_str("llm.model", "gpt-4o"),
            kv_str("llm.input", "hi"),
            kv_str("llm.output", "hello"),
            kv_int("llm.usage.prompt_tokens", 10),
            kv_int("llm.usage.completion_tokens", 20),
            kv_int("llm.usage.total_tokens", 30),
            kv_f64("llm.cost.total_usd", 0.003),
            kv_f64("llm.cost.prompt_usd", 0.001),
            kv_f64("llm.cost.completion_usd", 0.002),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        LlmConvention.apply(&view, &mut span);
        assert_eq!(span.llm_model, "gpt-4o");
        assert_eq!(span.llm_input, "hi");
        assert_eq!(span.llm_output, "hello");
        assert_eq!(span.prompt_tokens, 10);
        assert_eq!(span.completion_tokens, 20);
        assert_eq!(span.total_tokens, 30);
        assert!((span.total_cost_usd - 0.003).abs() < 1e-9);
        assert!((span.prompt_cost_usd - 0.001).abs() < 1e-9);
        assert!((span.completion_cost_usd - 0.002).abs() < 1e-9);
    }

    #[test]
    fn test_llm_convention_ignores_gen_ai_aliases() {
        let attrs = vec![kv_str("gen_ai.request.model", "claude")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        LlmConvention.apply(&view, &mut span);
        assert!(span.llm_model.is_empty());
    }
}
