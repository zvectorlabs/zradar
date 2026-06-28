//! Request context — tenant and project identity for each incoming request.
use crate::WorkspaceId;

/// Carries the workspace identity resolved for each request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    /// Workspace scope for data isolation.
    pub workspace_id: WorkspaceId,
}

impl RequestContext {
    pub fn new(workspace_id: WorkspaceId) -> Self {
        Self { workspace_id }
    }
}
