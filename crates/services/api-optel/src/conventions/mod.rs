//! Attribute-mapping conventions for OTLP → `Span` field translation.
//!
//! Each OpenTelemetry / vendor namespace lives in its own module behind the
//! [`AttributeConvention`] trait. `OtlpConverter` runs the conventions over a
//! borrowed [`AttrView`] in priority order and merges results into a single
//! `Span` per OTLP span — see TECH-SPEC-PHASE-0.md §4.2b and
//! TECH-SPEC-PHASE-1.md §3.6.
//!
//! **Allocation budget (must not regress):**
//! - 1 lazy `AttrView` index per span (`&'a str` keys, no string copies).
//! - K owning `to_string()` per `Span` field actually populated.
//! - 1 final `serde_json` serialization for the catch-all attributes column.
//!
//! Phase 1 adds `GenAiV1_29Convention`, `NatConvention`, and `AiqConvention`
//! to [`default_conventions`] (R1.2–R1.5). `EventsConvention` is added in PR6.

pub mod agent;
pub mod aiq;
pub mod attr_view;
pub mod db;
pub mod gen_ai_evaluation;
pub mod gen_ai_legacy;
pub mod gen_ai_memory;
pub mod gen_ai_task;
pub mod gen_ai_v1_29;
pub mod guardrails;
pub mod llm;
pub mod mcp;
pub mod nat;
pub mod openinference;
pub mod prompt;
pub mod resource;
pub mod sampling_params;
pub mod tool;
pub mod vertex;

pub use attr_view::{AnyValueRef, AttrView};
use zradar_models::Span;

/// A single OTel / vendor namespace's attribute-to-`Span`-field mapping.
///
/// Implementations are stateless and `Send + Sync` so the converter can hold
/// a shared `Vec<Box<dyn AttributeConvention>>` across worker threads.
///
/// `apply` reads attributes from a zero-copy [`AttrView`] borrowed from the
/// OTLP request buffer and writes into the partially-built `Span`. Conventions
/// must not clone attribute values during traversal — only `to_string` a
/// borrowed `&str` into the destination `Span` field.
pub trait AttributeConvention: Send + Sync {
    /// Apply this convention's mappings against `view`, populating `span`.
    fn apply(&self, view: &AttrView<'_>, span: &mut Span);
}

/// Default convention priority order (Phase 0 + Phase 1 R1.2–R1.5).
///
/// Most-specific namespaces run first so they win field-level conflicts.
///
///  1. [`openinference::OpenInferenceConvention`] — reserved OpenInference slot.
///  2. [`guardrails::GuardrailsConvention`] — `rail.*` / `action.*` (Phase 0).
///  3. [`agent::AgentConvention`] — generic `agent.*` / `user_id`.
///  4. [`vertex::VertexConvention`] — `gcp.vertex.agent.*` overrides.
///  5. [`llm::LlmConvention`] — canonical `llm.*` model/usage/cost.
///  6. [`gen_ai_v1_29::GenAiV1_29Convention`] — OTel GenAI 1.29:
///     `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
///     `gen_ai.response.model`, `gen_ai.provider.name` (Phase 1 R1.3–R1.5).
///     Runs **before** legacy so 1.29 keys take priority when both appear.
///  7. [`gen_ai_legacy::GenAiLegacyConvention`] — pre-1.29 `gen_ai.*` aliases.
///  8. [`nat::NatConvention`] — `nat.*` NeMo Agent Toolkit (Phase 1 R1.2).
///     Runs after generic agent conventions; `aiq.*` below can overwrite.
///  9. [`aiq::AiqConvention`] — canonical `aiq.*` alias for `nat.*` (Phase 1 R1.2).
///     Overwrites NAT values for `workflow_run_id` and `framework`.
/// 10. [`tool::ToolConvention`] — `tool.*` / `gen_ai.tool.*`.
/// 11. [`prompt::PromptConvention`] — `prompt.*` management.
/// 12. [`resource::ResourceConvention`] — `resource.*`, timing, versioning, level.
#[must_use]
pub fn default_conventions() -> Vec<Box<dyn AttributeConvention>> {
    vec![
        Box::new(openinference::OpenInferenceConvention),
        Box::new(guardrails::GuardrailsConvention),
        Box::new(agent::AgentConvention),
        Box::new(gen_ai_task::GenAiTaskConvention),
        Box::new(gen_ai_memory::GenAiMemoryConvention),
        Box::new(gen_ai_evaluation::GenAiEvaluationConvention),
        Box::new(vertex::VertexConvention),
        Box::new(llm::LlmConvention),
        // GenAI 1.29 before legacy — newer key names win when both present.
        Box::new(gen_ai_v1_29::GenAiV1_29Convention),
        Box::new(gen_ai_legacy::GenAiLegacyConvention),
        // NAT then AIQ: aiq.* canonical alias overwrites nat.* for shared fields.
        Box::new(nat::NatConvention),
        Box::new(aiq::AiqConvention),
        Box::new(db::DbConvention),
        Box::new(mcp::McpConvention),
        Box::new(tool::ToolConvention),
        Box::new(prompt::PromptConvention),
        Box::new(resource::ResourceConvention),
        // Phase 4 R4.4: collect gen_ai.request.* sampling params into
        // span.model_parameters JSON. Runs last so any earlier convention
        // that already touched these keys still wins (today none does).
        Box::new(sampling_params::SamplingParamsConvention),
    ]
}
