//! Legacy OpenTelemetry GenAI attribute aliases (pre-1.29).
//!
//! Owns: `gen_ai.request.model`, `gen_ai.prompt`, `gen_ai.completion`,
//! `gen_ai.usage.prompt_tokens`, `gen_ai.usage.completion_tokens`.
//!
//! Runs after [`crate::conventions::llm`] so legacy GenAI values override
//! canonical `llm.*` ones when both are present — matching the pre-refactor
//! cascade where they shared a single `gen_ai.request.model | llm.model`
//! match arm. Phase 1 will add a `GenAiV1_29Convention` for
//! `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` /
//! `gen_ai.response.model` / `gen_ai.provider.name` ahead of this one.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps legacy `gen_ai.*` attributes into `Span` fields.
pub struct GenAiLegacyConvention;

impl AttributeConvention for GenAiLegacyConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("gen_ai.request.model") {
            span.llm_model = v.to_string();
            view.mark_consumed("gen_ai.request.model");
        }
        if let Some(v) = view.get_str("gen_ai.prompt") {
            span.llm_input = v.to_string();
            view.mark_consumed("gen_ai.prompt");
        }
        if let Some(v) = view.get_str("gen_ai.completion") {
            span.llm_output = v.to_string();
            view.mark_consumed("gen_ai.completion");
        }
        // Token fields: GenAiV1_29Convention is the canonical source and runs
        // first. Only fall back to the legacy aliases if the canonical names
        // (`gen_ai.usage.input_tokens` / `output_tokens`) did not already
        // appear. Presence, not field value, is the sentinel: zero is a valid
        // canonical token count and must still win over legacy aliases.
        if !view.contains("gen_ai.usage.input_tokens")
            && let Some(v) = view.get_u64("gen_ai.usage.prompt_tokens")
        {
            span.prompt_tokens = v.min(i32::MAX as u64) as i32;
            view.mark_consumed("gen_ai.usage.prompt_tokens");
        }
        if !view.contains("gen_ai.usage.output_tokens")
            && let Some(v) = view.get_u64("gen_ai.usage.completion_tokens")
        {
            span.completion_tokens = v.min(i32::MAX as u64) as i32;
            view.mark_consumed("gen_ai.usage.completion_tokens");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conventions::gen_ai_v1_29::GenAiV1_29Convention;
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

    #[test]
    fn test_gen_ai_legacy_populates_fields() {
        let attrs = vec![
            kv_str("gen_ai.request.model", "claude-3"),
            kv_str("gen_ai.prompt", "p"),
            kv_str("gen_ai.completion", "c"),
            kv_int("gen_ai.usage.prompt_tokens", 5),
            kv_int("gen_ai.usage.completion_tokens", 7),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiLegacyConvention.apply(&view, &mut span);
        assert_eq!(span.llm_model, "claude-3");
        assert_eq!(span.llm_input, "p");
        assert_eq!(span.llm_output, "c");
        assert_eq!(span.prompt_tokens, 5);
        assert_eq!(span.completion_tokens, 7);
    }

    #[test]
    fn test_gen_ai_legacy_overwrites_existing_llm_values() {
        let attrs = vec![kv_str("gen_ai.request.model", "claude-3")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            llm_model: "from-llm-conv".to_string(),
            ..Span::default()
        };
        GenAiLegacyConvention.apply(&view, &mut span);
        // legacy runs after canonical llm.* and overwrites when present
        assert_eq!(span.llm_model, "claude-3");
    }

    /// P1-G9 regression: when both v1.29 and legacy token aliases appear on
    /// the same span, v1.29 wins. GenAiV1_29Convention runs first and writes
    /// `prompt_tokens` / `completion_tokens`; the legacy convention must not
    /// then overwrite those values from `gen_ai.usage.prompt_tokens` etc.
    #[test]
    fn test_gen_ai_legacy_does_not_overwrite_v1_29_tokens() {
        let attrs = vec![
            kv_int("gen_ai.usage.input_tokens", 10),
            kv_int("gen_ai.usage.output_tokens", 20),
            kv_int("gen_ai.usage.prompt_tokens", 99),
            kv_int("gen_ai.usage.completion_tokens", 77),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        GenAiLegacyConvention.apply(&view, &mut span);
        // v1.29 values stay; legacy aliases are ignored on a populated span.
        assert_eq!(span.prompt_tokens, 10);
        assert_eq!(span.completion_tokens, 20);
    }

    /// P1-G9 regression: zero is a valid v1.29 token value and must not be
    /// treated as "missing" when legacy aliases are also present.
    #[test]
    fn test_gen_ai_legacy_does_not_overwrite_zero_v1_29_tokens() {
        let attrs = vec![
            kv_int("gen_ai.usage.input_tokens", 0),
            kv_int("gen_ai.usage.output_tokens", 0),
            kv_int("gen_ai.usage.prompt_tokens", 99),
            kv_int("gen_ai.usage.completion_tokens", 77),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiV1_29Convention.apply(&view, &mut span);
        GenAiLegacyConvention.apply(&view, &mut span);

        assert_eq!(span.prompt_tokens, 0);
        assert_eq!(span.completion_tokens, 0);
    }

    /// A token count above `i32::MAX` saturates instead of wrapping negative,
    /// matching `GenAiV1_29Convention`.
    #[test]
    fn test_gen_ai_legacy_token_counts_saturate_at_i32_max() {
        let over = i32::MAX as i64 + 1;
        let attrs = vec![
            kv_int("gen_ai.usage.prompt_tokens", over),
            kv_int("gen_ai.usage.completion_tokens", over),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiLegacyConvention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, i32::MAX);
        assert_eq!(span.completion_tokens, i32::MAX);
    }

    /// P1-G9 regression: when only legacy aliases appear (no v1.29 keys),
    /// the legacy convention still populates from them. This is the fallback
    /// behavior for older SDKs.
    #[test]
    fn test_gen_ai_legacy_populates_when_v1_29_absent() {
        let attrs = vec![
            kv_int("gen_ai.usage.prompt_tokens", 5),
            kv_int("gen_ai.usage.completion_tokens", 7),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiLegacyConvention.apply(&view, &mut span);
        assert_eq!(span.prompt_tokens, 5);
        assert_eq!(span.completion_tokens, 7);
    }
}
