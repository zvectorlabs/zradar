//! Generic agent-context attribute mappings.
//!
//! Owns: `invocation_id`, `session_id`, `user_id`, `agent.name`, `agent.type`.
//! Vendor-specific `gcp.vertex.agent.*` aliases live in [`crate::conventions::vertex`]
//! and run after this convention so vendor values can override generic ones —
//! matching the pre-refactor monolithic-cascade semantics where both keys
//! shared a single match arm.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps generic `agent.*` and identity attributes into `Span` fields.
pub struct AgentConvention;

impl AttributeConvention for AgentConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        // invocation_id wire keys, in precedence order per D-G1:
        //  1. `zradar.invocation.id` — canonical zradar-prefixed dotted name
        //  2. `invocation.id` — generic dotted form
        //  3. `invocation_id` — legacy flat form
        if let Some(v) = view.get_str("zradar.invocation.id") {
            span.invocation_id = v.to_string();
            view.mark_consumed("zradar.invocation.id");
        } else if let Some(v) = view.get_str("invocation.id") {
            span.invocation_id = v.to_string();
            view.mark_consumed("invocation.id");
        } else if let Some(v) = view.get_str("invocation_id") {
            span.invocation_id = v.to_string();
            view.mark_consumed("invocation_id");
        }
        if let Some(v) = view.get_str("session_id") {
            span.session_id = v.to_string();
            view.mark_consumed("session_id");
        }
        if let Some(v) = view.get_str("user_id") {
            span.user_id = v.to_string();
            view.mark_consumed("user_id");
        }
        if let Some(v) = view.get_str("agent.name") {
            span.agent_name = v.to_string();
            view.mark_consumed("agent.name");
        }
        if let Some(v) = view.get_str("agent.type") {
            span.agent_type = v.to_string();
            view.mark_consumed("agent.type");
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
    fn test_agent_convention_populates_fields() {
        let attrs = vec![
            kv_str("invocation_id", "inv-1"),
            kv_str("session_id", "sess-2"),
            kv_str("user_id", "user-3"),
            kv_str("agent.name", "researcher"),
            kv_str("agent.type", "autonomous"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AgentConvention.apply(&view, &mut span);
        assert_eq!(span.invocation_id, "inv-1");
        assert_eq!(span.session_id, "sess-2");
        assert_eq!(span.user_id, "user-3");
        assert_eq!(span.agent_name, "researcher");
        assert_eq!(span.agent_type, "autonomous");
    }

    #[test]
    fn test_agent_convention_no_keys_leaves_span_untouched() {
        let attrs = vec![kv_str("other.key", "x")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AgentConvention.apply(&view, &mut span);
        assert!(span.agent_name.is_empty());
        assert!(span.invocation_id.is_empty());
    }

    /// P2-G3: `invocation.id` (OTel-dotted) maps into `invocation_id`.
    #[test]
    fn test_agent_convention_accepts_dotted_invocation_id() {
        let attrs = vec![kv_str("invocation.id", "inv-dotted")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AgentConvention.apply(&view, &mut span);
        assert_eq!(span.invocation_id, "inv-dotted");
    }

    /// P2-G3: `zradar.invocation.id` (canonical per D-G1) maps into
    /// `invocation_id`.
    #[test]
    fn test_agent_convention_accepts_zradar_dotted_invocation_id() {
        let attrs = vec![kv_str("zradar.invocation.id", "inv-zradar")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AgentConvention.apply(&view, &mut span);
        assert_eq!(span.invocation_id, "inv-zradar");
    }

    /// P2-G3: when all three keys appear, `zradar.invocation.id` wins
    /// per D-G1 (zradar-prefixed canonical).
    #[test]
    fn test_agent_convention_precedence_zradar_over_other_forms() {
        let attrs = vec![
            kv_str("invocation_id", "flat"),
            kv_str("invocation.id", "dotted"),
            kv_str("zradar.invocation.id", "zradar"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AgentConvention.apply(&view, &mut span);
        assert_eq!(span.invocation_id, "zradar");
    }
}
