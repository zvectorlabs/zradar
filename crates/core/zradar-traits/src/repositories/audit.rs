//! Audit logging trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct AuditLog {
    pub id: Uuid,
    pub organization_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub actor_type: Option<String>,  // 'user', 'api_key', 'system'
    pub actor_id: Option<Uuid>,
    pub actor_ip: Option<String>,
    pub action: String,
    pub resource_type: Option<String>,
    pub resource_id: Option<Uuid>,
    pub status: String,  // 'success', 'failure', 'permission_denied'
    pub details: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Audit event to be logged
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub organization_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub actor_type: Option<String>,
    pub actor_id: Option<Uuid>,
    pub actor_ip: Option<String>,
    pub action: String,
    pub resource_type: Option<String>,
    pub resource_id: Option<Uuid>,
    pub status: AuditStatus,
    pub details: Option<serde_json::Value>,
}

/// Audit status enum
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Success,
    Failure,
    PermissionDenied,
}

impl AuditStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditStatus::Success => "success",
            AuditStatus::Failure => "failure",
            AuditStatus::PermissionDenied => "permission_denied",
        }
    }
}

/// Trait for audit logging
#[async_trait]
pub trait AuditLogger: Send + Sync {
    /// Log an audit event
    async fn log(&self, event: AuditEvent) -> anyhow::Result<()>;
    
    /// Get audit logs with optional organization filter
    async fn get_logs(&self, org_id: Option<Uuid>, limit: Option<i64>) -> anyhow::Result<Vec<AuditLog>>;
}
