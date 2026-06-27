//! Resource-usage, timing, versioning, and severity attribute mappings.
//!
//! Owns: `resource.cpu.micros`, `resource.memory.bytes`, `resource.memory.peak`,
//! `time_to_first_token_ms` / `ttft_ms`, `agent.version`, `sdk.version`, `level`.
//!
//! These are the "miscellaneous" arms of the pre-refactor cascade that didn't
//! belong to a single instrumentation namespace. Grouping them in one module
//! keeps the per-convention modules small while preserving extension points
//! (e.g., Phase 1 may want to peel `agent.version` out to a future
//! `AgentConvention` extension).

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps resource-usage, timing, versioning, and severity attributes into
/// `Span` fields.
pub struct ResourceConvention;

impl AttributeConvention for ResourceConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_i64("resource.cpu.micros") {
            span.resource_cpu_micros = v;
            view.mark_consumed("resource.cpu.micros");
        }
        if let Some(v) = view.get_i64("resource.memory.bytes") {
            span.resource_memory_bytes = v;
            view.mark_consumed("resource.memory.bytes");
        }
        if let Some(v) = view.get_i64("resource.memory.peak") {
            span.resource_memory_peak = v;
            view.mark_consumed("resource.memory.peak");
        }
        if let Some(v) = view.get_u64("time_to_first_token_ms") {
            span.time_to_first_token_ms = v as i32;
            view.mark_consumed("time_to_first_token_ms");
        }
        if let Some(v) = view.get_u64("ttft_ms") {
            span.time_to_first_token_ms = v as i32;
            view.mark_consumed("ttft_ms");
        }
        if let Some(v) = view.get_str("agent.version") {
            span.agent_version = v.to_string();
            view.mark_consumed("agent.version");
        }
        if let Some(v) = view.get_str("sdk.version") {
            span.sdk_version = v.to_string();
            view.mark_consumed("sdk.version");
        }
        if let Some(v) = view.get_str("level") {
            span.level = v.to_string();
            view.mark_consumed("level");
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
    fn test_resource_convention_populates_all_known_fields() {
        let attrs = vec![
            kv_int("resource.cpu.micros", 1234),
            kv_int("resource.memory.bytes", 5678),
            kv_int("resource.memory.peak", 9999),
            kv_int("time_to_first_token_ms", 150),
            kv_str("agent.version", "1.0.0"),
            kv_str("sdk.version", "0.5.0"),
            kv_str("level", "DEBUG"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        ResourceConvention.apply(&view, &mut span);
        assert_eq!(span.resource_cpu_micros, 1234);
        assert_eq!(span.resource_memory_bytes, 5678);
        assert_eq!(span.resource_memory_peak, 9999);
        assert_eq!(span.time_to_first_token_ms, 150);
        assert_eq!(span.agent_version, "1.0.0");
        assert_eq!(span.sdk_version, "0.5.0");
        assert_eq!(span.level, "DEBUG");
    }

    #[test]
    fn test_ttft_ms_alias_populates_same_field() {
        let attrs = vec![kv_int("ttft_ms", 75)];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        ResourceConvention.apply(&view, &mut span);
        assert_eq!(span.time_to_first_token_ms, 75);
    }
}
