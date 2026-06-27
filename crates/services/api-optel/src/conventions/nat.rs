//! NeMo Agent Toolkit (NAT) `nat.*` attribute mappings (Phase 1 R1.2).
//!
//! Owns: `nat.workflow.run_id`, `nat.conversation.id`, `nat.framework`,
//! `nat.function.name`.
//!
//! Two-pass canonical-wins precedence: NAT fields are written first so that
//! later conventions (e.g. `AiqConvention`) can still overwrite them if the
//! `aiq.*` canonical alias is also present. Mapping:
//!
//! | OTel attribute           | `Span` field      | Notes                            |
//! |--------------------------|-------------------|----------------------------------|
//! | `nat.workflow.run_id`    | `workflow_run_id` | —                                |
//! | `nat.conversation.id`    | `session_id`      | only if `session_id` is empty    |
//! | `nat.framework`          | `framework`       | —                                |
//! | `nat.function.name`      | `agent_name`      | only if `agent_name` is empty    |
//!
//! Run **before** [`crate::conventions::gen_ai_legacy`] but **after** generic
//! conventions that may already populate `session_id` / `agent_name` from the
//! canonical `agent.*` namespace. See `default_conventions()`.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps `nat.*` NeMo Agent Toolkit attributes into `Span` fields.
pub struct NatConvention;

impl AttributeConvention for NatConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("nat.workflow.run_id") {
            span.workflow_run_id = v.to_string();
            view.mark_consumed("nat.workflow.run_id");
        }
        // Only populate session_id from NAT if not already set by a higher-priority convention.
        if span.session_id.is_empty()
            && let Some(v) = view.get_str("nat.conversation.id")
        {
            span.session_id = v.to_string();
            view.mark_consumed("nat.conversation.id");
        }
        if let Some(v) = view.get_str("nat.framework") {
            span.framework = v.to_string();
            view.mark_consumed("nat.framework");
        }
        // Only populate agent_name from NAT if not already set.
        if span.agent_name.is_empty()
            && let Some(v) = view.get_str("nat.function.name")
        {
            span.agent_name = v.to_string();
            view.mark_consumed("nat.function.name");
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
    fn test_nat_convention_populates_all_fields() {
        let attrs = vec![
            kv_str("nat.workflow.run_id", "wf-123"),
            kv_str("nat.conversation.id", "conv-abc"),
            kv_str("nat.framework", "langchain"),
            kv_str("nat.function.name", "summarizer"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        NatConvention.apply(&view, &mut span);
        assert_eq!(span.workflow_run_id, "wf-123");
        assert_eq!(span.session_id, "conv-abc");
        assert_eq!(span.framework, "langchain");
        assert_eq!(span.agent_name, "summarizer");
    }

    #[test]
    fn test_nat_convention_session_id_does_not_overwrite_existing() {
        let attrs = vec![kv_str("nat.conversation.id", "from-nat")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            session_id: "already-set".to_string(),
            ..Span::default()
        };
        NatConvention.apply(&view, &mut span);
        // Existing session_id wins; NAT value is ignored.
        assert_eq!(span.session_id, "already-set");
        // And should not be marked consumed since it wasn't applied.
        assert!(!view.is_consumed("nat.conversation.id"));
    }

    #[test]
    fn test_nat_convention_agent_name_does_not_overwrite_existing() {
        let attrs = vec![kv_str("nat.function.name", "from-nat")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            agent_name: "already-set".to_string(),
            ..Span::default()
        };
        NatConvention.apply(&view, &mut span);
        assert_eq!(span.agent_name, "already-set");
        assert!(!view.is_consumed("nat.function.name"));
    }

    #[test]
    fn test_nat_convention_workflow_run_id_always_overwrites() {
        let attrs = vec![kv_str("nat.workflow.run_id", "new-wf")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            workflow_run_id: "old-wf".to_string(),
            ..Span::default()
        };
        NatConvention.apply(&view, &mut span);
        assert_eq!(span.workflow_run_id, "new-wf");
        assert!(view.is_consumed("nat.workflow.run_id"));
    }

    #[test]
    fn test_nat_convention_missing_attrs_leaves_span_default() {
        let attrs = vec![kv_str("other.key", "ignored")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        NatConvention.apply(&view, &mut span);
        assert!(span.workflow_run_id.is_empty());
        assert!(span.session_id.is_empty());
        assert!(span.framework.is_empty());
        assert!(span.agent_name.is_empty());
    }

    #[test]
    fn test_nat_convention_marks_keys_consumed() {
        let attrs = vec![
            kv_str("nat.workflow.run_id", "wf-123"),
            kv_str("nat.framework", "autogen"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        NatConvention.apply(&view, &mut span);
        assert!(view.is_consumed("nat.workflow.run_id"));
        assert!(view.is_consumed("nat.framework"));
    }
}
