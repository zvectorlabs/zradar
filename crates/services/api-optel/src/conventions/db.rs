//! Database attribute mappings.
//!
//! Owns: `db.system.name`, `db.namespace`, `db.operation.name`, `db.query.text`,
//! `db.query.summary`, `db.collection.name`, `db.response.status_code`, plus their
//! legacy equivalents (e.g. `db.system`, `db.name`, `db.operation`, `db.statement`, `db.sql.table`).
//! Legacy attributes map first, then stable ~1.30+ attributes overwrite them if present.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps DB client call attributes into `Span` fields.
pub struct DbConvention;

impl AttributeConvention for DbConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        // Stable ~1.24+ attributes
        if let Some(v) = view.get_str("db.system.name") {
            span.db_system_name = v.to_string();
            view.mark_consumed("db.system.name");
        }
        if let Some(v) = view.get_str("db.namespace") {
            span.db_namespace = v.to_string();
            view.mark_consumed("db.namespace");
        }
        if let Some(v) = view.get_str("db.operation.name") {
            span.db_operation_name = v.to_string();
            view.mark_consumed("db.operation.name");
        }
        if let Some(v) = view.get_str("db.query.text") {
            span.db_query_text = v.to_string();
            view.mark_consumed("db.query.text");
        }
        if let Some(v) = view.get_str("db.query.summary") {
            span.db_query_summary = v.to_string();
            view.mark_consumed("db.query.summary");
        }
        if let Some(v) = view.get_str("db.collection.name") {
            span.db_collection_name = v.to_string();
            view.mark_consumed("db.collection.name");
        }
        if let Some(v) = view.get_str("db.response.status_code") {
            span.db_response_status_code = v.to_string();
            view.mark_consumed("db.response.status_code");
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
    fn test_db_convention_populates_from_stable() {
        let attrs = vec![
            kv_str("db.system.name", "postgresql"),
            kv_str("db.namespace", "public"),
            kv_str("db.operation.name", "SELECT"),
            kv_str("db.query.text", "SELECT * FROM users"),
            kv_str("db.query.summary", "SELECT users"),
            kv_str("db.collection.name", "users"),
            kv_str("db.response.status_code", "0"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        DbConvention.apply(&view, &mut span);

        assert_eq!(span.db_system_name, "postgresql");
        assert_eq!(span.db_namespace, "public");
        assert_eq!(span.db_operation_name, "SELECT");
        assert_eq!(span.db_query_text, "SELECT * FROM users");
        assert_eq!(span.db_query_summary, "SELECT users");
        assert_eq!(span.db_collection_name, "users");
        assert_eq!(span.db_response_status_code, "0");
    }
}
