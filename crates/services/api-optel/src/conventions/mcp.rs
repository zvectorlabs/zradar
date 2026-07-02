//! MCP (Model Context Protocol) tool call attribute mappings.
//!
//! Owns: `mcp.tool.name`, `mcp.server.name`, `mcp.tool.input`, `mcp.tool.output`.

use super::{AttrView, AttributeConvention};
use zradar_models::Span;

/// Maps MCP tool call attributes into `Span` fields.
pub struct McpConvention;

impl AttributeConvention for McpConvention {
    fn apply(&self, view: &AttrView<'_>, span: &mut Span) {
        if let Some(v) = view.get_str("mcp.tool.name") {
            span.mcp_tool_name = v.to_string();
            view.mark_consumed("mcp.tool.name");
        }
        if let Some(v) = view.get_str("mcp.server.name") {
            span.mcp_server_name = v.to_string();
            view.mark_consumed("mcp.server.name");
        }
        if let Some(v) = view.get_str("mcp.tool.input") {
            span.mcp_tool_input = v.to_string();
            view.mark_consumed("mcp.tool.input");
        }
        if let Some(v) = view.get_str("mcp.tool.output") {
            span.mcp_tool_output = v.to_string();
            view.mark_consumed("mcp.tool.output");
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
    fn test_mcp_convention_populates_fields() {
        let attrs = vec![
            kv_str("mcp.tool.name", "fetch_webpage"),
            kv_str("mcp.server.name", "web_crawler"),
            kv_str("mcp.tool.input", "{\"url\": \"https://example.com\"}"),
            kv_str("mcp.tool.output", "{\"html\": \"...\"}"),
        ];
        let view = AttrView::new(&attrs);
        let mut span = Span::default();
        McpConvention.apply(&view, &mut span);
        assert_eq!(span.mcp_tool_name, "fetch_webpage");
        assert_eq!(span.mcp_server_name, "web_crawler");
        assert_eq!(span.mcp_tool_input, "{\"url\": \"https://example.com\"}");
        assert_eq!(span.mcp_tool_output, "{\"html\": \"...\"}");
    }
}
