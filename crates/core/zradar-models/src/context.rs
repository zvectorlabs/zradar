//! Request context with authentication information

/// RequestContext contains authentication and authorization information
/// for each incoming request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub tenant_id: String,
    pub project_id: String,
    pub permissions: Vec<String>,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            tenant_id: "default".to_string(),
            project_id: "default".to_string(),
            permissions: vec![
                "write:traces".to_string(),
                "write:metrics".to_string(),
            ],
        }
    }
}

