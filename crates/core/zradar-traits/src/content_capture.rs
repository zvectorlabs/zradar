//! Content capture policy trait (R1.12).
//!
//! Controls whether LLM prompt/completion content is persisted per workspace.
//! When disabled, `llm_input` and `llm_output` are cleared before a span
//! reaches the write buffer.

use zradar_models::WorkspaceId;

/// Decides whether LLM content should be captured for a given workspace.
pub trait ContentCapturePolicy: Send + Sync {
    /// Returns `true` if llm_input/llm_output should be stored for this workspace.
    fn capture_enabled(&self, workspace_id: WorkspaceId) -> bool;
}

/// Always-capture implementation used when no per-workspace policy is configured.
pub struct NoopContentCapturePolicy;

impl ContentCapturePolicy for NoopContentCapturePolicy {
    fn capture_enabled(&self, _workspace_id: WorkspaceId) -> bool {
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
        assert!(policy.capture_enabled(Uuid::new_v4().into()));
    }
}
