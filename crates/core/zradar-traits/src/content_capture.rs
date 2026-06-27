//! Content capture policy trait (R1.12).
//!
//! Controls whether LLM prompt/completion content is persisted per project.
//! When disabled, `llm_input` and `llm_output` are cleared before a span
//! reaches the write buffer.

use uuid::Uuid;

/// Decides whether LLM content should be captured for a given project.
pub trait ContentCapturePolicy: Send + Sync {
    /// Returns `true` if llm_input/llm_output should be stored for this project.
    fn capture_enabled(&self, project_id: Uuid) -> bool;
}

/// Always-capture implementation used when no per-project policy is configured.
pub struct NoopContentCapturePolicy;

impl ContentCapturePolicy for NoopContentCapturePolicy {
    fn capture_enabled(&self, _project_id: Uuid) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_noop_always_captures() {
        let policy = NoopContentCapturePolicy;
        assert!(policy.capture_enabled(Uuid::new_v4()));
    }
}
