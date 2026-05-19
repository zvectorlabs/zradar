use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::{AuditLog, NewAuditLog};

#[derive(Debug, Clone, Default)]
pub struct AuditLogFilters {
    pub org_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub action: Option<String>,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub start_created_at: Option<i64>,
    pub end_created_at: Option<i64>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct AuditLogPage {
    pub items: Vec<AuditLog>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

#[async_trait]
pub trait AuditLogRepository: Send + Sync {
    async fn create_audit_log(&self, log: NewAuditLog) -> anyhow::Result<AuditLog>;
    async fn list_audit_logs(&self, filters: AuditLogFilters) -> anyhow::Result<AuditLogPage>;
}
