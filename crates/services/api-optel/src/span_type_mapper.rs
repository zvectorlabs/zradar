//! Span type detection from OTLP span attributes
//!
//! Detects semantic span types (GENERATION, TOOL, AGENT, etc.) from OpenTelemetry
//! span attributes using priority-based detection logic.

use serde_json::Value;
use std::collections::HashMap;

/// Mapper for detecting span types from OTLP attributes
pub struct SpanTypeMapper;

impl SpanTypeMapper {
    /// Detect span type from attributes and duration
    ///
    /// Uses priority-based detection:
    /// 1. Explicit zradar.span.type
    /// 2. OpenInference convention
    /// 3. GenAI semantic conventions
    /// 4. Vercel AI SDK patterns
    /// 5. Model-based heuristic
    /// 6. Tool detection
    /// 7. Agent detection
    /// 8. Zero duration = EVENT
    ///    Default: SPAN
    pub fn detect_type(attributes: &HashMap<String, Value>, duration_ns: i64) -> String {
        // Priority 1: Explicit zradar.span.type
        if let Some(explicit_type) = attributes.get("zradar.span.type")
            && let Some(type_str) = explicit_type.as_str()
        {
            return type_str.to_uppercase();
        }

        // Priority 2: openinference.span.kind
        if let Some(openinf) = attributes.get("openinference.span.kind")
            && let Some(kind) = openinf.as_str()
        {
            return match kind.to_uppercase().as_str() {
                "CHAIN" => "CHAIN",
                "RETRIEVER" => "RETRIEVER",
                "LLM" => "GENERATION",
                "EMBEDDING" => "EMBEDDING",
                "AGENT" => "AGENT",
                "TOOL" => "TOOL",
                "GUARDRAIL" => "GUARDRAIL",
                "EVALUATOR" => "EVALUATOR",
                _ => "SPAN",
            }
            .to_string();
        }

        // Priority 3: gen_ai.operation.name
        if let Some(gen_ai_op) = attributes.get("gen_ai.operation.name")
            && let Some(op) = gen_ai_op.as_str()
        {
            return match op {
                "chat" | "completion" | "generate_content" | "generate" => "GENERATION",
                "embeddings" => "EMBEDDING",
                "invoke_agent" | "create_agent" => "AGENT",
                "execute_tool" => "TOOL",
                _ => "SPAN",
            }
            .to_string();
        }

        // Priority 4: Vercel AI SDK patterns
        if let Some(op_name) = attributes
            .get("operation.name")
            .or_else(|| attributes.get("ai.operationId"))
            && let Some(op_str) = op_name.as_str()
        {
            if (op_str.starts_with("ai.generateText") || op_str.starts_with("ai.streamText"))
                && Self::has_model_info(attributes)
            {
                return "GENERATION".to_string();
            }
            if op_str.starts_with("ai.embed") && Self::has_model_info(attributes) {
                return "EMBEDDING".to_string();
            }
            if op_str.starts_with("ai.toolCall") {
                return "TOOL".to_string();
            }
        }

        // Priority 5: Model-based heuristic
        if Self::has_model_info(attributes) {
            return "GENERATION".to_string();
        }

        // Priority 6: Tool detection
        if Self::has_tool_info(attributes) {
            return "TOOL".to_string();
        }

        // Priority 7: Agent detection
        if Self::has_agent_info(attributes) {
            return "AGENT".to_string();
        }

        // Priority 8: Zero duration = EVENT
        if duration_ns == 0 {
            return "EVENT".to_string();
        }

        // Default
        "SPAN".to_string()
    }

    /// Check if attributes contain model information
    fn has_model_info(attributes: &HashMap<String, Value>) -> bool {
        attributes.contains_key("gen_ai.request.model")
            || attributes.contains_key("gen_ai.response.model")
            || attributes.contains_key("llm.model")
            || attributes.contains_key("ai.model.id")
    }

    /// Check if attributes contain tool information
    fn has_tool_info(attributes: &HashMap<String, Value>) -> bool {
        attributes.contains_key("tool.name") || attributes.contains_key("gen_ai.tool.name")
    }

    /// Check if attributes contain agent information
    fn has_agent_info(attributes: &HashMap<String, Value>) -> bool {
        attributes.contains_key("agent.name") || attributes.contains_key("agent.type")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_attrs(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn test_priority_1_explicit_zradar_type() {
        let attrs = make_attrs(vec![
            ("zradar.span.type", json!("CHAIN")),
            ("gen_ai.request.model", json!("gpt-4")), // Should be ignored
        ]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "CHAIN");
    }

    #[test]
    fn test_priority_1_explicit_zradar_type_lowercase() {
        let attrs = make_attrs(vec![("zradar.span.type", json!("generation"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_2_openinference_llm() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("LLM"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_2_openinference_retriever() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("RETRIEVER"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "RETRIEVER");
    }

    #[test]
    fn test_priority_2_openinference_chain() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("CHAIN"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "CHAIN");
    }

    #[test]
    fn test_priority_3_gen_ai_chat() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("chat"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_3_gen_ai_embeddings() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("embeddings"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "EMBEDDING");
    }

    #[test]
    fn test_priority_3_gen_ai_invoke_agent() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("invoke_agent"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "AGENT");
    }

    #[test]
    fn test_priority_4_vercel_generate_text() {
        let attrs = make_attrs(vec![
            ("operation.name", json!("ai.generateText.doGenerate")),
            ("ai.model.id", json!("gpt-4")),
        ]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_4_vercel_tool_call() {
        let attrs = make_attrs(vec![("ai.operationId", json!("ai.toolCall"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "TOOL");
    }

    #[test]
    fn test_priority_5_model_heuristic() {
        let attrs = make_attrs(vec![("gen_ai.request.model", json!("gpt-4"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_5_model_heuristic_response_model() {
        let attrs = make_attrs(vec![("gen_ai.response.model", json!("gpt-4"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "GENERATION");
    }

    #[test]
    fn test_priority_6_tool_detection() {
        let attrs = make_attrs(vec![("tool.name", json!("calculator"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "TOOL");
    }

    #[test]
    fn test_priority_6_tool_detection_gen_ai() {
        let attrs = make_attrs(vec![("gen_ai.tool.name", json!("calculator"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "TOOL");
    }

    #[test]
    fn test_priority_7_agent_detection() {
        let attrs = make_attrs(vec![("agent.name", json!("research-agent"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "AGENT");
    }

    #[test]
    fn test_priority_7_agent_detection_type() {
        let attrs = make_attrs(vec![("agent.type", json!("autonomous"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "AGENT");
    }

    #[test]
    fn test_priority_8_zero_duration_event() {
        let attrs = HashMap::new();
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 0), "EVENT");
    }

    #[test]
    fn test_default_span_type() {
        let attrs = HashMap::new();
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "SPAN");
    }

    #[test]
    fn test_priority_order_explicit_overrides_all() {
        let attrs = make_attrs(vec![
            ("zradar.span.type", json!("EVALUATOR")),
            ("openinference.span.kind", json!("LLM")),
            ("gen_ai.request.model", json!("gpt-4")),
            ("tool.name", json!("calculator")),
        ]);
        // Explicit type should win despite other attributes
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "EVALUATOR");
    }

    #[test]
    fn test_priority_order_openinference_over_model() {
        let attrs = make_attrs(vec![
            ("openinference.span.kind", json!("RETRIEVER")),
            ("gen_ai.request.model", json!("gpt-4")),
        ]);
        // OpenInference should win over model heuristic
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000), "RETRIEVER");
    }
}
