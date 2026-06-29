//! NeMo Guardrails attribute mappings (Phase 0 R0.2 – R0.4; Phase 4 R4.2).
//!
//! Owns: `rail.type`, `rail.name`, `rail.stop`, `action.name`,
//! `action.has_llm_calls`, `action.llm_calls_count`, `llm.cache.hit`.
//!
//! These first-class fields back the typed columns added in PR4
//! (`zradar-plans/nemo-compatibility/techspec/TECH-SPEC-PHASE-0.md` §4).
//! Booleans (`rail.stop`, `action.has_llm_calls`) are stored as `i16`
//! (`0` / `1`) for PostgreSQL `SMALLINT` compatibility, matching the
//! existing `is_deleted` pattern in the `Span` struct.
//!
//! Convention placement: dispatched **before** generic conventions in
//! [`crate::conventions::default_conventions`] so that the Guardrails
//! namespace claims its keys first. See TECH-SPEC-PHASE-0.md §4 for the
//! ordering rationale.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps NeMo Guardrails `rail.*` and `action.*` attributes into `Span` fields.
pub struct GuardrailsConvention;

impl AttributeConvention for GuardrailsConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("rail.type") {
            span.rail_type = v.to_string();
            view.mark_consumed("rail.type");
        }
        if let Some(v) = view.get_str("rail.name") {
            span.rail_name = v.to_string();
            view.mark_consumed("rail.name");
        }
        // `rail.stop` is a bool in canonical OTLP NeMo Guardrails emissions
        // (per TECH-SPEC-PHASE-0.md §3). We only accept the typed `BoolValue`
        // shape — string-encoded "true" / "false" are intentionally ignored
        // and left to the JSON catch-all column. This keeps the first-class
        // field honest about its provenance.
        if let Some(v) = view.get_bool("rail.stop") {
            span.rail_stop = i16::from(v);
            view.mark_consumed("rail.stop");
        }
        if let Some(v) = view.get_str("action.name") {
            span.action_name = v.to_string();
            view.mark_consumed("action.name");
        }
        if let Some(v) = view.get_bool("action.has_llm_calls") {
            span.action_has_llm_calls = i16::from(v);
            view.mark_consumed("action.has_llm_calls");
        }
        // `action.llm_calls_count` is an integer in canonical emissions.
        // Accept `IntValue` only; saturate to i32 range.
        if let Some(v) = view.get_i64("action.llm_calls_count") {
            span.action_llm_calls_count = v.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
            view.mark_consumed("action.llm_calls_count");
        }
        // Phase 4 R4.2: NeMo Guardrails extension `llm.cache.hit` — a bool
        // marking whether the inner LLM call hit the cache. Stored as a
        // tri-state i16: the `Span` default is `-1` (unknown/never reported);
        // an explicit bool here maps to `0` (miss) or `1` (hit) so hit-RATE
        // analytics can tell a real miss apart from a span with no cache info.
        if let Some(v) = view.get_bool("llm.cache.hit") {
            span.llm_cache_hit = i16::from(v);
            view.mark_consumed("llm.cache.hit");
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

    fn kv_bool(k: &str, v: bool) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(v)),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_guardrails_convention_populates_all_fields() {
        let attrs = vec![
            kv_str("rail.type", "input"),
            kv_str("rail.name", "self_check_input"),
            kv_bool("rail.stop", true),
            kv_str("action.name", "self_check_input"),
            kv_bool("action.has_llm_calls", true),
            kv_int("action.llm_calls_count", 3),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.rail_type, "input");
        assert_eq!(span.rail_name, "self_check_input");
        assert_eq!(span.rail_stop, 1);
        assert_eq!(span.action_name, "self_check_input");
        assert_eq!(span.action_has_llm_calls, 1);
        assert_eq!(span.action_llm_calls_count, 3);
    }

    #[test]
    fn test_guardrails_convention_bool_false_yields_zero() {
        let attrs = vec![
            kv_bool("rail.stop", false),
            kv_bool("action.has_llm_calls", false),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.rail_stop, 0);
        assert_eq!(span.action_has_llm_calls, 0);
    }

    #[test]
    fn test_guardrails_convention_missing_attrs_leaves_span_default() {
        let attrs = vec![kv_str("other.key", "ignored")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert!(span.rail_type.is_empty());
        assert!(span.rail_name.is_empty());
        assert_eq!(span.rail_stop, 0);
        assert!(span.action_name.is_empty());
        assert_eq!(span.action_has_llm_calls, 0);
        assert_eq!(span.action_llm_calls_count, 0);
    }

    #[test]
    fn test_guardrails_convention_ignores_wrong_typed_bool() {
        // `rail.stop` arrives as a string instead of bool — by design we
        // skip it rather than guess. The catch-all JSON column will still
        // mirror the raw value (the converter does that after dispatch).
        let attrs = vec![kv_str("rail.stop", "true")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.rail_stop, 0);
    }

    #[test]
    fn test_guardrails_convention_ignores_wrong_typed_int() {
        // `action.llm_calls_count` arrives as a string — skipped, leaving the
        // field at default 0.
        let attrs = vec![kv_str("action.llm_calls_count", "5")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.action_llm_calls_count, 0);
    }

    #[test]
    fn test_guardrails_convention_marks_keys_consumed() {
        let attrs = vec![
            kv_str("rail.type", "input"),
            kv_bool("rail.stop", true),
            kv_int("action.llm_calls_count", 2),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert!(view.is_consumed("rail.type"));
        assert!(view.is_consumed("rail.stop"));
        assert!(view.is_consumed("action.llm_calls_count"));
        // Keys not present in attrs are not consumed.
        assert!(!view.is_consumed("rail.name"));
    }

    /// Phase 4 R4.2 / AC4.3: `llm.cache.hit=true` populates `llm_cache_hit=1`.
    #[test]
    fn test_guardrails_convention_llm_cache_hit_true() {
        let attrs = vec![kv_bool("llm.cache.hit", true)];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.llm_cache_hit, 1);
        assert!(view.is_consumed("llm.cache.hit"));
    }

    #[test]
    fn test_guardrails_convention_llm_cache_hit_false() {
        let attrs = vec![kv_bool("llm.cache.hit", false)];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.llm_cache_hit, 0);
    }

    /// String-encoded "true" is intentionally ignored — bool-only contract.
    /// The field stays at its `-1` unknown default (NOT `0`, which would mean
    /// an explicit cache miss).
    #[test]
    fn test_guardrails_convention_llm_cache_hit_string_ignored() {
        let attrs = vec![kv_str("llm.cache.hit", "true")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GuardrailsConvention.apply(&view, &mut span);
        assert_eq!(span.llm_cache_hit, -1);
    }
}
