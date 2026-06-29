//! Tool-call attribute mappings.
//!
//! Owns: `tool.name`, `tool.call.id`, `gen_ai.tool.name`, `gen_ai.tool.call.id`.
//! These shared a single match arm in the pre-refactor cascade
//! (`gen_ai.tool.name | tool.name`); this convention consolidates them and
//! prefers the canonical `tool.*` first, then the GenAI alias second so the
//! alias wins on conflict — matching the original last-write-wins semantics.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps tool-call attributes into `Span` fields.
pub struct ToolConvention;

impl AttributeConvention for ToolConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("tool.name") {
            span.tool_name = v.to_string();
            view.mark_consumed("tool.name");
        }
        if let Some(v) = view.get_str("gen_ai.tool.name") {
            span.tool_name = v.to_string();
            view.mark_consumed("gen_ai.tool.name");
        }
        if let Some(v) = view.get_str("tool.call.id") {
            span.tool_call_id = v.to_string();
            view.mark_consumed("tool.call.id");
        }
        if let Some(v) = view.get_str("gen_ai.tool.call.id") {
            span.tool_call_id = v.to_string();
            view.mark_consumed("gen_ai.tool.call.id");
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

    #[test]
    fn test_tool_convention_populates_from_canonical() {
        let attrs = vec![
            kv_str("tool.name", "calculator"),
            kv_str("tool.call.id", "call-1"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        ToolConvention.apply(&view, &mut span);
        assert_eq!(span.tool_name, "calculator");
        assert_eq!(span.tool_call_id, "call-1");
    }

    #[test]
    fn test_tool_convention_populates_from_gen_ai_alias() {
        let attrs = vec![
            kv_str("gen_ai.tool.name", "calculator"),
            kv_str("gen_ai.tool.call.id", "call-2"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        ToolConvention.apply(&view, &mut span);
        assert_eq!(span.tool_name, "calculator");
        assert_eq!(span.tool_call_id, "call-2");
    }

    #[test]
    fn test_tool_convention_gen_ai_alias_wins_on_conflict() {
        let attrs = vec![
            kv_str("tool.name", "canonical"),
            kv_str("gen_ai.tool.name", "alias"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        ToolConvention.apply(&view, &mut span);
        assert_eq!(span.tool_name, "alias");
    }
}
