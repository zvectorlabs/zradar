//! Prompt-management attribute mappings.
//!
//! Owns: `prompt.id`, `prompt.name`, `prompt.version`.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps `prompt.*` attributes into `Span` fields.
pub struct PromptConvention;

impl AttributeConvention for PromptConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("prompt.id") {
            span.prompt_id = v.to_string();
            view.mark_consumed("prompt.id");
        }
        if let Some(v) = view.get_str("prompt.name") {
            span.prompt_name = v.to_string();
            view.mark_consumed("prompt.name");
        }
        if let Some(v) = view.get_u64("prompt.version") {
            span.prompt_version = v as i32;
            view.mark_consumed("prompt.version");
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

    #[test]
    fn test_prompt_convention_populates_fields() {
        let attrs = vec![
            kv_str("prompt.id", "p-1"),
            kv_str("prompt.name", "summary"),
            kv_int("prompt.version", 3),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        PromptConvention.apply(&view, &mut span);
        assert_eq!(span.prompt_id, "p-1");
        assert_eq!(span.prompt_name, "summary");
        assert_eq!(span.prompt_version, 3);
    }

    #[test]
    fn test_prompt_convention_ignores_unrelated() {
        let attrs = vec![kv_str("agent.name", "x")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        PromptConvention.apply(&view, &mut span);
        assert!(span.prompt_id.is_empty());
        assert!(span.prompt_name.is_empty());
        assert_eq!(span.prompt_version, 0);
    }
}
