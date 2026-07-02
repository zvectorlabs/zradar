//! OTel GenAI SIG task-lifecycle attribute mappings.
//!
//! Owns: `gen_ai.task.id`, `gen_ai.task.parent.id`, `gen_ai.task.name`,
//! `gen_ai.task.kind`, `gen_ai.task.state`, `gen_ai.task.status`.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps OTel GenAI task-lifecycle attributes into `Span` fields.
pub struct GenAiTaskConvention;

impl AttributeConvention for GenAiTaskConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("gen_ai.task.id") {
            span.agent_task_id = v.to_string();
            view.mark_consumed("gen_ai.task.id");
        }
        if let Some(v) = view.get_str("gen_ai.task.parent.id") {
            span.agent_task_parent_id = v.to_string();
            view.mark_consumed("gen_ai.task.parent.id");
        }
        if let Some(v) = view.get_str("gen_ai.task.name") {
            span.agent_task_name = v.to_string();
            view.mark_consumed("gen_ai.task.name");
        }
        if let Some(v) = view.get_str("gen_ai.task.kind") {
            span.agent_task_kind = v.to_string();
            view.mark_consumed("gen_ai.task.kind");
        }
        if let Some(v) = view.get_str("gen_ai.task.state") {
            span.agent_task_state = v.to_string();
            view.mark_consumed("gen_ai.task.state");
        }
        if let Some(v) = view.get_str("gen_ai.task.status") {
            span.agent_task_status = v.to_string();
            view.mark_consumed("gen_ai.task.status");
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
    fn test_task_convention_populates_fields() {
        let attrs = vec![
            kv_str("gen_ai.task.id", "task-123"),
            kv_str("gen_ai.task.parent.id", "parent-456"),
            kv_str("gen_ai.task.name", "solve_issue"),
            kv_str("gen_ai.task.kind", "planning"),
            kv_str("gen_ai.task.state", "in-progress"),
            kv_str("gen_ai.task.status", "success"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        GenAiTaskConvention.apply(&view, &mut span);
        assert_eq!(span.agent_task_id, "task-123");
        assert_eq!(span.agent_task_parent_id, "parent-456");
        assert_eq!(span.agent_task_name, "solve_issue");
        assert_eq!(span.agent_task_kind, "planning");
        assert_eq!(span.agent_task_state, "in-progress");
        assert_eq!(span.agent_task_status, "success");
    }
}
