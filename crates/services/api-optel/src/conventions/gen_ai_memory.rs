//! OTel GenAI SIG memory-store attribute mappings.
//!
//! Owns: `gen_ai.memory.type`, `gen_ai.memory.key`.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps OTel GenAI memory-store attributes into `Span` fields.
pub struct GenAiMemoryConvention;

impl AttributeConvention for GenAiMemoryConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("gen_ai.memory.type") {
            span.memory_type = v.to_string();
            view.mark_consumed("gen_ai.memory.type");
        }
        if let Some(v) = view.get_str("gen_ai.memory.key") {
            span.memory_key = v.to_string();
            view.mark_consumed("gen_ai.memory.key");
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
    fn test_memory_convention_populates_fields() {
        let attrs = vec![
            kv_str("gen_ai.memory.type", "vector_db"),
            kv_str("gen_ai.memory.key", "user_context_cache"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiMemoryConvention.apply(&view, &mut span);
        assert_eq!(span.memory_type, "vector_db");
        assert_eq!(span.memory_key, "user_context_cache");
    }
}
