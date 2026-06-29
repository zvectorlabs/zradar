//! OTel GenAI semantic conventions 1.29 (Phase 1 R1.3 + R1.4 + R1.5; Phase 4 R4.3).
//!
//! Owns the new token-count keys and response/provider fields introduced in
//! OpenTelemetry GenAI 1.29:
//!
//! | OTel attribute               | `Span` field         | Notes                              |
//! |------------------------------|----------------------|------------------------------------|
//! | `gen_ai.usage.input_tokens`  | `prompt_tokens`      | alias for `prompt_tokens`          |
//! | `gen_ai.usage.output_tokens` | `completion_tokens`  | alias for `completion_tokens`      |
//! | `gen_ai.response.model`      | `llm_response_model` | response model (not request model) |
//! | `gen_ai.provider.name`       | `llm_provider`       | provider attribution               |
//! | `gen_ai.response.id`         | `llm_response_id`    | join key to provider-side logs (R4.3) |
//!
//! **Ordering:** runs **before** [`crate::conventions::gen_ai_legacy`] so that
//! the 1.29 keys take priority. If both `gen_ai.usage.input_tokens` (1.29)
//! and `gen_ai.usage.prompt_tokens` (legacy) appear in the same span, the
//! 1.29 key wins because this convention is dispatched first.
//!
//! `gen_ai.response.model` maps to `llm_response_model` — a distinct field
//! from `llm_model` (`gen_ai.request.model`) per TECH-SPEC-PHASE-1.md §3.4.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps OTel GenAI 1.29 attributes into `Span` fields.
pub struct GenAiV1_29Convention;

impl AttributeConvention for GenAiV1_29Convention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_u64("gen_ai.usage.input_tokens") {
            span.prompt_tokens = v.min(i32::MAX as u64) as i32;
            view.mark_consumed("gen_ai.usage.input_tokens");
        }
        if let Some(v) = view.get_u64("gen_ai.usage.output_tokens") {
            span.completion_tokens = v.min(i32::MAX as u64) as i32;
            view.mark_consumed("gen_ai.usage.output_tokens");
        }
        if let Some(v) = view.get_str("gen_ai.response.model") {
            span.llm_response_model = v.to_string();
            view.mark_consumed("gen_ai.response.model");
        }
        if let Some(v) = view.get_str("gen_ai.provider.name") {
            span.llm_provider = v.to_string();
            view.mark_consumed("gen_ai.provider.name");
        }
        // Phase 4 R4.3: gen_ai.response.id → llm_response_id (provider log join).
        if let Some(v) = view.get_str("gen_ai.response.id") {
            span.llm_response_id = v.to_string();
            view.mark_consumed("gen_ai.response.id");
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
            ..Default::default()
        }
    }

    fn kv_int(k: &str, v: i64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(v)),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_gen_ai_v1_29_populates_all_fields() {
        let attrs = vec![
            kv_int("gen_ai.usage.input_tokens", 100),
            kv_int("gen_ai.usage.output_tokens", 50),
            kv_str("gen_ai.response.model", "claude-3-sonnet-20240229"),
            kv_str("gen_ai.provider.name", "anthropic"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, 100);
        assert_eq!(span.completion_tokens, 50);
        assert_eq!(span.llm_response_model, "claude-3-sonnet-20240229");
        assert_eq!(span.llm_provider, "anthropic");
    }

    #[test]
    fn test_gen_ai_v1_29_response_model_distinct_from_request_model() {
        // gen_ai.response.model goes to llm_response_model, NOT llm_model.
        let attrs = vec![kv_str("gen_ai.response.model", "claude-3-opus")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            llm_model: "claude-3-sonnet".to_string(), // from gen_ai.request.model
            ..Span::default()
        };
        GenAiV1_29Convention.apply(&view, &mut span);
        // llm_model (request model) must not be touched.
        assert_eq!(span.llm_model, "claude-3-sonnet");
        assert_eq!(span.llm_response_model, "claude-3-opus");
    }

    #[test]
    fn test_gen_ai_v1_29_input_tokens_wins_over_legacy_prompt_tokens() {
        // GenAiV1_29Convention runs before GenAiLegacyConvention. Simulate:
        // 1.29 sets prompt_tokens = 100.
        // Legacy would try to set 80, but since 1.29 already ran and
        // consumed the key, legacy's get_u64 on prompt_tokens would find a
        // different key name anyway. Here we just verify 1.29 writes correctly.
        let attrs = vec![kv_int("gen_ai.usage.input_tokens", 100)];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, 100);
        assert!(view.is_consumed("gen_ai.usage.input_tokens"));
    }

    #[test]
    fn test_gen_ai_v1_29_negative_int_ignored_for_u64_tokens() {
        // get_u64 returns None for negative IntValue, leaving tokens at 0.
        let attrs = vec![kv_int("gen_ai.usage.input_tokens", -1)];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, 0);
    }

    #[test]
    fn test_gen_ai_v1_29_missing_attrs_leaves_span_default() {
        let attrs = vec![kv_str("other.key", "ignored")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, 0);
        assert_eq!(span.completion_tokens, 0);
        assert!(span.llm_response_model.is_empty());
        assert!(span.llm_provider.is_empty());
    }

    #[test]
    fn test_gen_ai_v1_29_marks_keys_consumed() {
        let attrs = vec![
            kv_int("gen_ai.usage.input_tokens", 10),
            kv_str("gen_ai.provider.name", "openai"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert!(view.is_consumed("gen_ai.usage.input_tokens"));
        assert!(view.is_consumed("gen_ai.provider.name"));
    }

    /// Phase 4 R4.3 / AC4.4: `gen_ai.response.id` → `llm_response_id`.
    #[test]
    fn test_gen_ai_v1_29_response_id_populates_llm_response_id() {
        let attrs = vec![kv_str("gen_ai.response.id", "chatcmpl-abc123")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert_eq!(span.llm_response_id, "chatcmpl-abc123");
        assert!(view.is_consumed("gen_ai.response.id"));
    }

    #[test]
    fn test_gen_ai_v1_29_no_response_id_leaves_empty() {
        let attrs = vec![kv_str("gen_ai.response.model", "claude-3")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        assert!(
            span.llm_response_id.is_empty(),
            "missing gen_ai.response.id must leave llm_response_id empty"
        );
    }
}
