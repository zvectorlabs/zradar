//! NVIDIA AIQ (Agent Intelligence Quotient) `aiq.*` attribute mappings (Phase 1 R1.2).
//!
//! The `aiq.*` namespace is the canonical alias for `nat.*` introduced in
//! NeMo Agent Toolkit ≥ 0.3.x. When both namespaces appear the `aiq.*` value
//! wins by running **after** [`crate::conventions::nat::NatConvention`] and
//! unconditionally overwriting the same fields (except `session_id` /
//! `agent_name`, which still respect the "first written wins" rule for fields
//! that can be legitimately sourced from multiple places).
//!
//! | OTel attribute            | `Span` field      | Notes                            |
//! |---------------------------|-------------------|----------------------------------|
//! | `aiq.workflow.run_id`     | `workflow_run_id` | overwrites NAT value if present  |
//! | `aiq.conversation.id`     | `session_id`      | only if `session_id` is empty    |
//! | `aiq.framework`           | `framework`       | overwrites NAT value if present  |
//! | `aiq.function.name`       | `agent_name`      | only if `agent_name` is empty    |

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps `aiq.*` NeMo/AIQ canonical attributes into `Span` fields.
///
/// Runs after [`crate::conventions::nat::NatConvention`] so that `aiq.*`
/// values overwrite `nat.*` values for the same fields.
pub struct AiqConvention;

impl AttributeConvention for AiqConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        // workflow_run_id: aiq.* always overwrites (canonical namespace wins).
        if let Some(v) = view.get_str("aiq.workflow.run_id") {
            span.workflow_run_id = v.to_string();
            view.mark_consumed("aiq.workflow.run_id");
        }
        // session_id: respect "first writer wins" — don't overwrite if already set.
        if span.session_id.is_empty()
            && let Some(v) = view.get_str("aiq.conversation.id")
        {
            span.session_id = v.to_string();
            view.mark_consumed("aiq.conversation.id");
        }
        // framework: aiq.* always overwrites.
        if let Some(v) = view.get_str("aiq.framework") {
            span.framework = v.to_string();
            view.mark_consumed("aiq.framework");
        }
        // agent_name: respect "first writer wins".
        if span.agent_name.is_empty()
            && let Some(v) = view.get_str("aiq.function.name")
        {
            span.agent_name = v.to_string();
            view.mark_consumed("aiq.function.name");
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
    fn test_aiq_convention_populates_all_fields() {
        let attrs = vec![
            kv_str("aiq.workflow.run_id", "aiq-wf-456"),
            kv_str("aiq.conversation.id", "aiq-conv-xyz"),
            kv_str("aiq.framework", "langgraph"),
            kv_str("aiq.function.name", "planner"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AiqConvention.apply(&view, &mut span);
        assert_eq!(span.workflow_run_id, "aiq-wf-456");
        assert_eq!(span.session_id, "aiq-conv-xyz");
        assert_eq!(span.framework, "langgraph");
        assert_eq!(span.agent_name, "planner");
    }

    #[test]
    fn test_aiq_overwrites_nat_workflow_run_id() {
        // Simulates NatConvention having already run and set nat.* values.
        let attrs = vec![kv_str("aiq.workflow.run_id", "aiq-wf")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            workflow_run_id: "nat-wf".to_string(),
            ..Span::default()
        };
        AiqConvention.apply(&view, &mut span);
        assert_eq!(span.workflow_run_id, "aiq-wf");
    }

    #[test]
    fn test_aiq_overwrites_nat_framework() {
        let attrs = vec![kv_str("aiq.framework", "aiq-framework")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            framework: "nat-framework".to_string(),
            ..Span::default()
        };
        AiqConvention.apply(&view, &mut span);
        assert_eq!(span.framework, "aiq-framework");
    }

    #[test]
    fn test_aiq_session_id_does_not_overwrite_existing() {
        let attrs = vec![kv_str("aiq.conversation.id", "aiq-conv")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            session_id: "already-set".to_string(),
            ..Span::default()
        };
        AiqConvention.apply(&view, &mut span);
        assert_eq!(span.session_id, "already-set");
        assert!(!view.is_consumed("aiq.conversation.id"));
    }

    #[test]
    fn test_aiq_agent_name_does_not_overwrite_existing() {
        let attrs = vec![kv_str("aiq.function.name", "aiq-agent")];
        let view = AttrView::new(&attrs);
        let mut span = Span {
            agent_name: "existing-agent".to_string(),
            ..Span::default()
        };
        AiqConvention.apply(&view, &mut span);
        assert_eq!(span.agent_name, "existing-agent");
        assert!(!view.is_consumed("aiq.function.name"));
    }

    #[test]
    fn test_aiq_missing_attrs_leaves_span_default() {
        let attrs = vec![kv_str("other.key", "ignored")];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AiqConvention.apply(&view, &mut span);
        assert!(span.workflow_run_id.is_empty());
        assert!(span.session_id.is_empty());
        assert!(span.framework.is_empty());
        assert!(span.agent_name.is_empty());
    }

    #[test]
    fn test_aiq_marks_keys_consumed() {
        let attrs = vec![
            kv_str("aiq.workflow.run_id", "wf"),
            kv_str("aiq.framework", "fw"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        AiqConvention.apply(&view, &mut span);
        assert!(view.is_consumed("aiq.workflow.run_id"));
        assert!(view.is_consumed("aiq.framework"));
    }
}
