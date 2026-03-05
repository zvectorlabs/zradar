//! Span data model for distributed tracing

use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// Span represents a single unit of work in distributed tracing.
///
/// This includes standard OpenTelemetry fields plus LLM-specific attributes
/// for tracking costs, tokens, prompts, and model parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Row, sqlx::FromRow)]
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
    // Hierarchy (Two-Level Multi-tenancy)
    // ============================================================
    pub tenant_id: String,  // Organization/Team
    pub project_id: String, // Project within org

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
    ];

    /// Validate if a string is a valid span type
    pub fn validate_span_type(type_str: &str) -> bool {
        Self::VALID_SPAN_TYPES.contains(&type_str)
    }

    /// Check if this span is generation-like (could include LLM calls)
    pub fn is_generation_like(&self) -> bool {
        matches!(
            self.span_type.as_str(),
            "GENERATION"
                | "AGENT"
                | "TOOL"
                | "CHAIN"
                | "RETRIEVER"
                | "EVALUATOR"
                | "EMBEDDING"
                | "GUARDRAIL"
        )
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
            tenant_id: String::new(),
            project_id: String::new(),
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
    }

    #[test]
    fn test_validate_span_type_invalid() {
        assert!(!Span::validate_span_type("INVALID"));
        assert!(!Span::validate_span_type("generation")); // case sensitive
        assert!(!Span::validate_span_type(""));
        assert!(!Span::validate_span_type("SPAN ")); // whitespace
    }

    #[test]
    fn test_is_generation_like() {
        let mut span = Span {
            span_type: "GENERATION".to_string(),
            ..Span::default()
        };
        assert!(span.is_generation_like());

        span.span_type = "TOOL".to_string();
        assert!(span.is_generation_like());

        span.span_type = "AGENT".to_string();
        assert!(span.is_generation_like());

        span.span_type = "CHAIN".to_string();
        assert!(span.is_generation_like());

        span.span_type = "RETRIEVER".to_string();
        assert!(span.is_generation_like());

        span.span_type = "EVALUATOR".to_string();
        assert!(span.is_generation_like());

        span.span_type = "EMBEDDING".to_string();
        assert!(span.is_generation_like());

        span.span_type = "GUARDRAIL".to_string();
        assert!(span.is_generation_like());

        span.span_type = "SPAN".to_string();
        assert!(!span.is_generation_like());

        span.span_type = "EVENT".to_string();
        assert!(!span.is_generation_like());
    }

    #[test]
    fn test_default_span_type() {
        let span = Span::default();
        assert_eq!(span.span_type, "SPAN");
    }
}
