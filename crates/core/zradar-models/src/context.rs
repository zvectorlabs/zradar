//! Request context — tenant and project identity for each incoming request.

/// Carries the tenant and project identity resolved from the API key.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub tenant_id: String,
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
