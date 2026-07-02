//! Span type detection from OTLP span attributes
//!
//! Detects semantic span types (GENERATION, TOOL, AGENT, etc.) from OpenTelemetry
//! span attributes using priority-based detection logic.

use serde_json::Value;

use crate::conventions::AttrView;

/// Read-only attribute lookups needed by span-type detection.
///
/// Implemented for both the borrowed [`AttrView`] (the zero-copy ingest path)
/// and `serde_json::Map` (the unit tests), so `detect_type` runs against either
/// without building an intermediate map.
pub(crate) trait AttrSource {
    /// The string value for `key`, if present and a string.
    fn lookup_str(&self, key: &str) -> Option<&str>;
    /// Whether `key` is present (any value kind).
    fn has_key(&self, key: &str) -> bool;
    /// Whether any key starts with `prefix`.
    fn has_key_prefix(&self, prefix: &str) -> bool;
}

impl AttrSource for AttrView<'_> {
    fn lookup_str(&self, key: &str) -> Option<&str> {
        self.get_str(key)
    }
    fn has_key(&self, key: &str) -> bool {
        self.contains(key)
    }
    fn has_key_prefix(&self, prefix: &str) -> bool {
        self.iter().any(|(k, _)| k.starts_with(prefix))
    }
}

impl AttrSource for serde_json::Map<String, Value> {
    fn lookup_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }
    fn has_key(&self, key: &str) -> bool {
        self.contains_key(key)
    }
    fn has_key_prefix(&self, prefix: &str) -> bool {
        self.keys().any(|k| k.starts_with(prefix))
    }
}

/// Mapper for detecting span types from OTLP attributes
pub struct SpanTypeMapper;

impl SpanTypeMapper {
    /// Detect span type from attributes, duration, and span name.
    ///
    /// Uses priority-based detection:
    /// 1. Explicit zradar.span.type
    /// 2. Guardrails-explicit (NeMo Guardrails R0.2 – R0.4); historically
    ///    documented as rule 1.5 in TECH-SPEC-PHASE-0.md §4
    /// 3. OpenInference convention
    /// 4. GenAI semantic conventions
    /// 5. Vercel AI SDK patterns
    /// 6. Model-based heuristic
    /// 7. Tool detection
    /// 8. Agent detection
    /// 9. Zero duration = EVENT
    ///
    /// Default: SPAN
    pub(crate) fn detect_type<A: AttrSource>(
        attributes: &A,
        duration_ns: i64,
        span_name: &str,
        service_name: &str,
    ) -> String {
        // Priority 1: Explicit zradar.span.type
        if let Some(type_str) = attributes.lookup_str("zradar.span.type") {
            return type_str.to_uppercase();
        }

        // Priority 1.5: Guardrails-explicit (Phase 0 R0.2 – R0.4).
        //
        // Runs **before** the OpenInference rule so a Guardrails-owned LLM
        // child carrying both `rail.type` and `openinference.span.kind=LLM`
        // is typed as `GUARDRAIL` rather than `GENERATION` — keeping the
        // Guardrails frame visually coherent in the UI (see
        // TECH-SPEC-PHASE-0.md §4).
        if Self::is_guardrails(attributes, span_name, service_name) {
            return "GUARDRAIL".to_string();
        }

        // Priority 2: openinference.span.kind
        if let Some(kind) = attributes.lookup_str("openinference.span.kind") {
            return match kind.to_uppercase().as_str() {
                "CHAIN" => "CHAIN",
                "RETRIEVER" => "RETRIEVER",
                "LLM" => "GENERATION",
                "EMBEDDING" => "EMBEDDING",
                "AGENT" => "AGENT",
                "TOOL" => "TOOL",
                "GUARDRAIL" => "GUARDRAIL",
                "EVALUATOR" => "EVALUATOR",
                "RERANKER" => "RERANKER",
                _ => "SPAN",
            }
            .to_string();
        }

        // Priority 3: gen_ai.operation.name
        if let Some(op) = attributes.lookup_str("gen_ai.operation.name") {
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
        if let Some(op_str) = attributes
            .lookup_str("operation.name")
            .or_else(|| attributes.lookup_str("ai.operationId"))
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

        // Priority 4.5: Database spans
        if Self::has_db_info(attributes) {
            return "DATABASE".to_string();
        }

        // Priority 5: Model-based heuristic
        if Self::has_model_info(attributes) {
            return "GENERATION".to_string();
        }

        // Priority 5.5: Evaluation detection
        if Self::has_evaluation_info(attributes) {
            return "EVALUATOR".to_string();
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
    fn has_model_info<A: AttrSource>(attributes: &A) -> bool {
        attributes.has_key("gen_ai.request.model")
            || attributes.has_key("gen_ai.response.model")
            || attributes.has_key("llm.model")
            || attributes.has_key("ai.model.id")
    }

    /// Check if attributes contain evaluation information
    fn has_evaluation_info<A: AttrSource>(attributes: &A) -> bool {
        attributes.has_key("gen_ai.evaluation.name")
    }

    /// Check if attributes contain tool information
    fn has_tool_info<A: AttrSource>(attributes: &A) -> bool {
        attributes.has_key("tool.name")
            || attributes.has_key("gen_ai.tool.name")
            || attributes.has_key("mcp.tool.name")
    }

    /// Check if attributes contain database information
    fn has_db_info<A: AttrSource>(attributes: &A) -> bool {
        attributes.has_key_prefix("db.")
    }

    /// Check if attributes contain agent information
    fn has_agent_info<A: AttrSource>(attributes: &A) -> bool {
        attributes.has_key("agent.name") || attributes.has_key("agent.type")
    }

    /// Detect whether a span is unambiguously emitted by NeMo Guardrails.
    ///
    /// Per DECISIONS.md `OQ19`, the rules are:
    /// - `span_name` starts with the reserved `guardrails.` prefix → GUARDRAIL.
    /// - `gen_ai.operation.name` equals `"guardrails"` → GUARDRAIL.
    /// - `rail.*` attribute present (`rail.type`, `rail.name`, etc.) → GUARDRAIL.
    /// - `action.name` present **AND** Guardrails context: span name starts
    ///   with `guardrails.`, service is `nemo_guardrails`, or any `rail.*`
    ///   attribute is present.
    ///
    /// `action.name` alone is too broad — LangChain and AutoGen also emit it.
    fn is_guardrails<A: AttrSource>(attrs: &A, span_name: &str, service_name: &str) -> bool {
        // Standalone markers (any one suffices per OQ19):
        if span_name.starts_with("guardrails.") {
            return true;
        }
        if let Some(op) = attrs.lookup_str("gen_ai.operation.name")
            && op == "guardrails"
        {
            return true;
        }
        if attrs.has_key_prefix("rail.") {
            return true;
        }
        // OQ19 compound predicate: `action.name` alone is too broad
        // (LangChain/AutoGen also emit it). It needs a Guardrails context
        // marker — the span-name and rail.* markers above already cover
        // two of the three, so only the service marker remains.
        attrs.has_key("action.name") && service_name == "nemo_guardrails"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_attrs(pairs: Vec<(&str, Value)>) -> serde_json::Map<String, Value> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn test_priority_1_explicit_zradar_type() {
        let attrs = make_attrs(vec![
            ("zradar.span.type", json!("CHAIN")),
            ("gen_ai.request.model", json!("gpt-4")), // Should be ignored
        ]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "CHAIN");
    }

    #[test]
    fn test_priority_1_explicit_zradar_type_lowercase() {
        let attrs = make_attrs(vec![("zradar.span.type", json!("generation"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_2_openinference_llm() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("LLM"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_2_openinference_retriever() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("RETRIEVER"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "RETRIEVER"
        );
    }

    #[test]
    fn test_priority_2_openinference_reranker() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("RERANKER"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "RERANKER"
        );
    }

    #[test]
    fn test_priority_2_openinference_chain() {
        let attrs = make_attrs(vec![("openinference.span.kind", json!("CHAIN"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "CHAIN");
    }

    #[test]
    fn test_priority_3_gen_ai_chat() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("chat"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_3_gen_ai_embeddings() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("embeddings"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "EMBEDDING"
        );
    }

    #[test]
    fn test_priority_3_gen_ai_invoke_agent() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("invoke_agent"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "AGENT");
    }

    #[test]
    fn test_priority_4_vercel_generate_text() {
        let attrs = make_attrs(vec![
            ("operation.name", json!("ai.generateText.doGenerate")),
            ("ai.model.id", json!("gpt-4")),
        ]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_4_vercel_tool_call() {
        let attrs = make_attrs(vec![("ai.operationId", json!("ai.toolCall"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "TOOL");
    }

    #[test]
    fn test_priority_5_model_heuristic() {
        let attrs = make_attrs(vec![("gen_ai.request.model", json!("gpt-4"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_4_5_database() {
        // Any db.* attribute should map to DATABASE
        let attrs = make_attrs(vec![("db.system.name", json!("postgresql"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "DATABASE"
        );
    }

    #[test]
    fn test_priority_5_model_heuristic_response_model() {
        let attrs = make_attrs(vec![("gen_ai.response.model", json!("gpt-4"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GENERATION"
        );
    }

    #[test]
    fn test_priority_6_tool_detection() {
        let attrs = make_attrs(vec![("tool.name", json!("calculator"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "TOOL");
    }

    #[test]
    fn test_priority_6_tool_detection_gen_ai() {
        let attrs = make_attrs(vec![("gen_ai.tool.name", json!("calculator"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "TOOL");
    }

    #[test]
    fn test_priority_6_tool_detection_mcp() {
        let attrs = make_attrs(vec![("mcp.tool.name", json!("fetch_webpage"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "TOOL");
    }

    #[test]
    fn test_priority_5_5_evaluation_detection() {
        let attrs = make_attrs(vec![("gen_ai.evaluation.name", json!("safety_check"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "EVALUATOR"
        );
    }

    #[test]
    fn test_priority_7_agent_detection() {
        let attrs = make_attrs(vec![("agent.name", json!("research-agent"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "AGENT");
    }

    #[test]
    fn test_priority_7_agent_detection_type() {
        let attrs = make_attrs(vec![("agent.type", json!("autonomous"))]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "AGENT");
    }

    #[test]
    fn test_priority_8_zero_duration_event() {
        let attrs = serde_json::Map::new();
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 0, "", ""), "EVENT");
    }

    #[test]
    fn test_default_span_type() {
        let attrs = serde_json::Map::new();
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "SPAN");
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
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "EVALUATOR"
        );
    }

    #[test]
    fn test_priority_order_openinference_over_model() {
        let attrs = make_attrs(vec![
            ("openinference.span.kind", json!("RETRIEVER")),
            ("gen_ai.request.model", json!("gpt-4")),
        ]);
        // OpenInference should win over model heuristic
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "RETRIEVER"
        );
    }

    // -----------------------------------------------------------------
    // Priority 1.5: NeMo Guardrails-explicit (Phase 0 R0.2 – R0.4)
    // -----------------------------------------------------------------

    #[test]
    fn test_priority_1_5_guardrails_op_name() {
        let attrs = make_attrs(vec![("gen_ai.operation.name", json!("guardrails"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GUARDRAIL"
        );
    }

    #[test]
    fn test_priority_1_5_guardrails_span_name() {
        let attrs = serde_json::Map::new();
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "guardrails.request", ""),
            "GUARDRAIL"
        );
    }

    #[test]
    fn test_priority_1_5_guardrails_rail_type() {
        let attrs = make_attrs(vec![("rail.type", json!("input"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GUARDRAIL"
        );
    }

    /// OQ19: `action.name` alone is too broad (LangChain/AutoGen also emit it)
    /// and must NOT classify as GUARDRAIL without a Guardrails context marker
    /// (guardrails.* span name, nemo_guardrails service, or any rail.* attr).
    #[test]
    fn test_priority_1_5_guardrails_action_name_alone_not_guardrail() {
        let attrs = make_attrs(vec![("action.name", json!("self_check_input"))]);
        // No span-name prefix, no service marker, no rail.* — must fall
        // through to default SPAN, not classify as GUARDRAIL.
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "SPAN");
    }

    /// OQ19: action.name + nemo_guardrails service → GUARDRAIL.
    #[test]
    fn test_priority_1_5_guardrails_action_name_with_service() {
        let attrs = make_attrs(vec![("action.name", json!("self_check_input"))]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", "nemo_guardrails"),
            "GUARDRAIL"
        );
    }

    /// OQ19: action.name + rail.* attribute → GUARDRAIL (rail.* alone already
    /// triggers; this covers the explicit compound case).
    #[test]
    fn test_priority_1_5_guardrails_action_name_with_rail() {
        let attrs = make_attrs(vec![
            ("action.name", json!("self_check_input")),
            ("rail.type", json!("input")),
        ]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GUARDRAIL"
        );
    }

    #[test]
    fn test_priority_1_5_runs_before_openinference() {
        // Guardrails-owned LLM child: rail.type + openinference.span.kind=LLM.
        // Rule 1.5 must fire first so the span stays typed as GUARDRAIL
        // (keeping the Guardrails frame visually coherent in the UI per
        // TECH-SPEC-PHASE-0.md §4) instead of being demoted to CHAIN/GENERATION.
        let attrs = make_attrs(vec![
            ("rail.type", json!("output")),
            ("openinference.span.kind", json!("CHAIN")),
        ]);
        assert_eq!(
            SpanTypeMapper::detect_type(&attrs, 1000, "", ""),
            "GUARDRAIL"
        );
    }

    #[test]
    fn test_priority_1_5_explicit_zradar_type_still_wins_over_guardrails() {
        // Belt-and-braces: rule 1 (explicit zradar.span.type) must still
        // outrank rule 1.5 — if a user has pinned the type, respect it.
        let attrs = make_attrs(vec![
            ("zradar.span.type", json!("CHAIN")),
            ("rail.type", json!("input")),
        ]);
        assert_eq!(SpanTypeMapper::detect_type(&attrs, 1000, "", ""), "CHAIN");
    }
}
