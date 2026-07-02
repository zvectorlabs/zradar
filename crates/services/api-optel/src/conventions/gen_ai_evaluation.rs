//! OTel GenAI evaluation semantic conventions.
//!
//! Owns:
//! - `gen_ai.evaluation.name` -> `evaluation_name`
//! - `gen_ai.evaluation.explanation` -> `evaluation_explanation`
//! - `gen_ai.evaluation.passed` -> `evaluation_passed`

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

pub struct GenAiEvaluationConvention;

impl AttributeConvention for GenAiEvaluationConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("gen_ai.evaluation.name") {
            span.evaluation_name = v.to_string();
            view.mark_consumed("gen_ai.evaluation.name");
        }
        if let Some(v) = view.get_str("gen_ai.evaluation.explanation") {
            span.evaluation_explanation = v.to_string();
            view.mark_consumed("gen_ai.evaluation.explanation");
        }
        if let Some(v) = view.get_bool("gen_ai.evaluation.passed") {
            span.evaluation_passed = i16::from(v);
            view.mark_consumed("gen_ai.evaluation.passed");
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
    fn test_evaluation_convention_populates_fields() {
        let attrs = vec![
            kv_str("gen_ai.evaluation.name", "toxicity_check"),
            kv_str(
                "gen_ai.evaluation.explanation",
                "Response contains offensive language",
            ),
            kv_bool("gen_ai.evaluation.passed", false),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();

        GenAiEvaluationConvention.apply(&view, &mut span);

        assert_eq!(span.evaluation_name, "toxicity_check");
        assert_eq!(
            span.evaluation_explanation,
            "Response contains offensive language"
        );
        assert_eq!(span.evaluation_passed, 0);

        assert!(view.is_consumed("gen_ai.evaluation.name"));
        assert!(view.is_consumed("gen_ai.evaluation.explanation"));
        assert!(view.is_consumed("gen_ai.evaluation.passed"));
    }
}
