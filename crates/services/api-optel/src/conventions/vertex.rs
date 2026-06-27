//! Google Cloud Vertex AI agent attribute mappings.
//!
//! Owns the `gcp.vertex.agent.*` aliases extracted from the original
//! `map_attribute_to_span` cascade. Runs after [`crate::conventions::agent`]
//! so vendor-specific values overwrite generic ones — matching the
//! pre-refactor cascade where both keys (`session_id |
//! gcp.vertex.agent.session_id`) shared a single match arm and last-write-wins
//! depended on OTLP attribute ordering. Putting vertex after agent gives a
//! deterministic "vendor wins" outcome when both are present.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps `gcp.vertex.agent.*` attributes into `Span` fields.
pub struct VertexConvention;

impl AttributeConvention for VertexConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("gcp.vertex.agent.invocation_id") {
            span.invocation_id = v.to_string();
            view.mark_consumed("gcp.vertex.agent.invocation_id");
        }
        if let Some(v) = view.get_str("gcp.vertex.agent.session_id") {
            span.session_id = v.to_string();
            view.mark_consumed("gcp.vertex.agent.session_id");
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

    #[test]
    fn test_vertex_convention_populates_fields() {
        let attrs = vec![
            kv_str("gcp.vertex.agent.invocation_id", "v-inv"),
            kv_str("gcp.vertex.agent.session_id", "v-sess"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        VertexConvention.apply(&view, &mut span);
        assert_eq!(span.invocation_id, "v-inv");
        assert_eq!(span.session_id, "v-sess");
    }

    #[test]
    fn test_vertex_convention_ignores_generic_keys() {
        let attrs = vec![kv_str("invocation_id", "generic")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        VertexConvention.apply(&view, &mut span);
        assert!(span.invocation_id.is_empty());
    }
}
