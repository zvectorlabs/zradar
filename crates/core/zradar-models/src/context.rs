//! Request context — tenant and project identity for each incoming request.

/// Carries the tenant and project identity resolved for each request,
/// along with optional user-level context forwarded by the Agnitiv gateway
/// in platform mode.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Agnitiv `org_id` mapped to zradar tenant scope.
    pub tenant_id: String,
    /// Agnitiv `project_id` for data-plane isolation.
    pub project_id: String,
    /// Agnitiv `user_id` (sub claim). Present in platform mode; empty in standalone.
    pub user_id: String,
    /// Agnitiv org slug for human-readable audit logs. Present in platform mode; empty in standalone.
    pub org_slug: String,
    /// zradar-scoped permissions forwarded by Agnitiv (`zradar:*`).
    /// Empty slice in standalone mode; used for M02 route-level enforcement in platform mode.
    pub permissions: Vec<String>,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            tenant_id: "default".to_string(),
            project_id: "default".to_string(),
            user_id: String::new(),
            org_slug: String::new(),
            permissions: Vec::new(),
        }
    }
}
