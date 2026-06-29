//! Audit query service trait.
//!
//! Abstracts audit-log query operations using the existing repository types
//! (`AuditLogFilters`, `AuditLogPage`).

use async_trait::async_trait;
use zradar_models::WorkspaceId;

use crate::errors::ServiceError;
use crate::repositories::audit_log::{AuditLogFilters, AuditLogPage};

/// Audit log query service trait.
#[async_trait]
pub trait AuditQueryService: Send + Sync {
    /// List audit log entries matching the given filters.
    async fn list_audit_logs(
        &self,
        workspace_id: WorkspaceId,
        filters: AuditLogFilters,
    ) -> Result<AuditLogPage, ServiceError>;
}
