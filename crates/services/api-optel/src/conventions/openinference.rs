//! OpenInference attribute mappings.
//!
//! Today no `openinference.*` attribute populates a dedicated `Span` field
//! at the converter layer — `openinference.span.kind` is consumed by
//! [`crate::SpanTypeMapper`] for `span_type` detection (priority 2 of the
//! detector cascade) and is left in the JSON attribute bag, not promoted.
//!
//! This module exists as the Phase 0 scaffolding slot per
//! TECH-SPEC-PHASE-0.md §4.2b so future OpenInference attribute promotions
//! (e.g., `openinference.session.id`, `openinference.user.id`) have a
//! dedicated home rather than getting bolted onto another convention.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps `openinference.*` attributes into `Span` fields. Currently a no-op —
/// see module-level docs.
pub struct OpenInferenceConvention;

impl AttributeConvention for OpenInferenceConvention {
    fn apply(&self, _view: &AttrView<'_>, _span: &mut Span) {
        // No attribute-side mappings in Phase 0 PR3. Reserved slot.
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
    fn test_openinference_convention_is_currently_noop() {
        let attrs = vec![kv_str("openinference.span.kind", "LLM")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        OpenInferenceConvention.apply(&view, &mut span);
        // No fields populated; span_type detection lives in SpanTypeMapper.
        assert_eq!(span.span_type, "SPAN");
        assert!(span.agent_name.is_empty());
    }
}
