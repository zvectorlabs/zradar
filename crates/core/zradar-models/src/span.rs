//! Span data model for distributed tracing

use serde::{Deserialize, Serialize};

/// Span represents a single unit of work in distributed tracing.
///
/// This includes standard OpenTelemetry fields plus LLM-specific attributes
/// for tracking costs, tokens, prompts, and model parameters.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Span {
    // ============================================================
    // Identity
    // ============================================================
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,

    // ============================================================
    // Timing
    // ============================================================
    pub timestamp: i64,   // Unix nanoseconds
    pub duration_ns: i64, // Duration in nanoseconds (i64 for PostgreSQL compat)

    // ============================================================
    // Hierarchy (Workspace Scope)
    // ============================================================
    pub workspace_id: String, // Unified isolation boundary

    // ============================================================
    // Service Metadata
    // ============================================================
    pub service_name: String,
    pub span_name: String,
    pub span_kind: String, // INTERNAL, CLIENT, SERVER, etc.
    pub span_type: String, // SPAN, EVENT, GENERATION, AGENT, TOOL, etc.

    // ============================================================
    // Status
    // ============================================================
    pub status_code: String, // UNSET, OK, ERROR
    pub status_message: String,

    // ============================================================
    // Agent Context (Commonly Queried)
    // ============================================================
    pub invocation_id: String,
    pub session_id: String,
    pub user_id: String,
    pub agent_name: String,
    pub agent_type: String,

    // ============================================================
    // LLM-Specific Fields
    // ============================================================
    pub llm_model: String,
    pub llm_input: String,  // Prompt (compressed in DB)
    pub llm_output: String, // Completion (compressed in DB)
    pub prompt_tokens: i32, // i32 for PostgreSQL compat
    pub completion_tokens: i32,
    pub total_tokens: i32,

    // ============================================================
    // Cost Tracking
    // ============================================================
    pub prompt_cost_usd: f64,
    pub completion_cost_usd: f64,
    pub total_cost_usd: f64,

    // ============================================================
    // Tool-Specific
    // ============================================================
    pub tool_name: String,
    pub tool_call_id: String,

    // ============================================================
    // Guardrails (Phase 0: NeMo Guardrails R0.2 – R0.4)
    // ============================================================
    pub rail_type: String,
    pub rail_name: String,
    pub rail_stop: i16, // bool stored as SMALLINT for PostgreSQL compat
    pub action_name: String,
    pub action_has_llm_calls: i16, // bool stored as SMALLINT for PG compat
    pub action_llm_calls_count: i32,

    // ============================================================
    // NeMo Agent Toolkit + OTel GenAI 1.29 (Phase 1: R1.2 – R1.6)
    // ============================================================
    pub workflow_run_id: String, // nat.workflow.run_id / aiq.workflow.run_id
    pub framework: String,       // nat.framework / aiq.framework
    pub llm_provider: String,    // gen_ai.provider.name
    pub llm_response_model: String, // gen_ai.response.model (request model stays in llm_model)
    pub events: String,          // JSON: serialized span-event allowlist

    // ============================================================
    // Phase 4 polish columns (R4.2 – R4.6)
    // ============================================================
    // Tri-state SMALLINT for NeMo `llm.cache.hit`: `-1` = unknown/absent,
    // `0` = explicit cache miss, `1` = explicit cache hit. The unknown state
    // is distinct from an explicit miss so cache-hit-RATE analytics can use
    // (hits) / (hits + misses) without counting spans that never reported.
    pub llm_cache_hit: i16,
    pub llm_response_id: String, // gen_ai.response.id (provider-side log join)
    pub environment: String,     // deployment.environment resource attribute
    pub links: String,           // JSON: OTLP span links array

    // ============================================================
    // Resource Attributes (From Profiling)
    // ============================================================
    pub resource_cpu_micros: i64, // i64 for PostgreSQL compat
    pub resource_memory_bytes: i64,
    pub resource_memory_peak: i64,

    // ============================================================
    // Prompt Management
    // ============================================================
    pub prompt_id: String,
    pub prompt_name: String,
    pub prompt_version: i32, // i32 for PostgreSQL compat

    // ============================================================
    // Timing Details
    // ============================================================
    pub completion_start_time: Option<i64>,
    pub time_to_first_token_ms: i32, // i32 for PostgreSQL compat

    // ============================================================
    // Versioning
    // ============================================================
    pub agent_version: String,
    pub sdk_version: String,

    // ============================================================
    // Level/Severity
    // ============================================================
    pub level: String, // DEBUG, INFO, WARNING, ERROR, CRITICAL

    // ============================================================
    // Flexible Attributes (JSON)
    // ============================================================
    pub model_parameters: String, // JSON: {"temperature": 0.7, ...}
    pub attributes: String,       // JSON: All other key-value pairs

    // ============================================================
    // Record Lifecycle
    // ============================================================
    pub created_at: i64,
    pub updated_at: i64,
    pub is_deleted: i16, // i16 for PostgreSQL compat (SMALLINT)
}

impl Span {
    /// Valid span types that can be used
    pub const VALID_SPAN_TYPES: &'static [&'static str] = &[
        "SPAN",
        "EVENT",
        "GENERATION",
        "AGENT",
        "TOOL",
        "CHAIN",
        "RETRIEVER",
        "EVALUATOR",
        "EMBEDDING",
        "GUARDRAIL",
        "RERANKER",
    ];

    /// Validate if a string is a valid span type
    pub fn validate_span_type(type_str: &str) -> bool {
        Self::VALID_SPAN_TYPES.contains(&type_str)
    }
}

impl Default for Span {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            trace_id: String::new(),
            span_id: String::new(),
            parent_span_id: String::new(),
            timestamp: 0,
            duration_ns: 0, // i64
            workspace_id: String::new(),
            service_name: String::new(),
            span_name: String::new(),
            span_kind: "INTERNAL".to_string(),
            span_type: "SPAN".to_string(),
            status_code: "UNSET".to_string(),
            status_message: String::new(),
            invocation_id: String::new(),
            session_id: String::new(),
            user_id: String::new(),
            agent_name: String::new(),
            agent_type: String::new(),
            llm_model: String::new(),
            llm_input: String::new(),
            llm_output: String::new(),
            prompt_tokens: 0,     // i32
            completion_tokens: 0, // i32
            total_tokens: 0,      // i32
            prompt_cost_usd: 0.0,
            completion_cost_usd: 0.0,
            total_cost_usd: 0.0,
            tool_name: String::new(),
            tool_call_id: String::new(),
            rail_type: String::new(),
            rail_name: String::new(),
            rail_stop: 0, // i16 (bool false)
            action_name: String::new(),
            action_has_llm_calls: 0,   // i16 (bool false)
            action_llm_calls_count: 0, // i32
            workflow_run_id: String::new(),
            framework: String::new(),
            llm_provider: String::new(),
            llm_response_model: String::new(),
            events: "[]".to_string(), // JSON empty array
            llm_cache_hit: -1,        // tri-state: -1 unknown, 0 miss, 1 hit
            llm_response_id: String::new(),
            environment: String::new(),
            links: "[]".to_string(),  // JSON empty array
            resource_cpu_micros: 0,   // i64
            resource_memory_bytes: 0, // i64
            resource_memory_peak: 0,  // i64
            prompt_id: String::new(),
            prompt_name: String::new(),
            prompt_version: 0, // i32
            completion_start_time: None,
            time_to_first_token_ms: 0, // i32
            agent_version: String::new(),
            sdk_version: String::new(),
            level: "INFO".to_string(),
            model_parameters: "{}".to_string(),
            attributes: "{}".to_string(),
            created_at: now,
            updated_at: now,
            is_deleted: 0, // i16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_span_type_valid() {
        assert!(Span::validate_span_type("SPAN"));
        assert!(Span::validate_span_type("GENERATION"));
        assert!(Span::validate_span_type("TOOL"));
        assert!(Span::validate_span_type("AGENT"));
        assert!(Span::validate_span_type("CHAIN"));
        assert!(Span::validate_span_type("RETRIEVER"));
        assert!(Span::validate_span_type("EVALUATOR"));
        assert!(Span::validate_span_type("EMBEDDING"));
        assert!(Span::validate_span_type("GUARDRAIL"));
        assert!(Span::validate_span_type("EVENT"));
        assert!(Span::validate_span_type("RERANKER"));
    }

    #[test]
    fn test_validate_span_type_invalid() {
        assert!(!Span::validate_span_type("INVALID"));
        assert!(!Span::validate_span_type("generation")); // case sensitive
        assert!(!Span::validate_span_type(""));
        assert!(!Span::validate_span_type("SPAN ")); // whitespace
    }

    #[test]
    fn test_default_span_type() {
        let span = Span::default();
        assert_eq!(span.span_type, "SPAN");
    }
}
