//! Request context — tenant and project identity for each incoming request.

/// Carries the tenant and project identity resolved for each request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Tenant scope for data isolation.
    pub tenant_id: String,
    /// Project scope for data isolation.
    pub project_id: String,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            tenant_id: "default".to_string(),
            project_id: "default".to_string(),
        }
    }
}
