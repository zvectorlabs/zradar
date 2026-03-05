//! Log record data model

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// LogRecord represents a single log entry from OTLP logs export.
///
/// Fields are per PRD Section 6.4.
#[derive(Debug, Clone, Serialize, Deserialize, Default, sqlx::FromRow)]
pub struct LogRecord {
    // Identity
    pub id: String, // UUID as string

    // Timing (nanoseconds since epoch)
    pub timestamp: i64,

    // Multi-tenancy
    pub tenant_id: String,
    pub project_id: String,

    // Correlation with traces
    pub trace_id: String, // empty string if no correlation
    pub span_id: String,  // empty string if no correlation

    // Log severity
    pub severity: String, // DEBUG, INFO, WARN, ERROR, FATAL, TRACE, etc.

    // Service metadata
    pub service_name: String,

    // Log message (body)
    pub message: String,

    // JSON-serialized OTLP attributes
    pub attributes: String,

    // JSON-serialized resource attributes
    pub resource: String,

    // Agent context (optional)
    pub agent_name: String,   // empty string if not present
    pub session_id: String,   // empty string if not present
    pub user_id: String,      // empty string if not present

    // Lifecycle
    pub created_at: i64,
}

impl LogRecord {
    /// Create a new LogRecord with a generated UUID.
    pub fn new(tenant_id: impl Into<String>, project_id: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.into(),
            project_id: project_id.into(),
            created_at: now,
            ..Default::default()
        }
    }
}
