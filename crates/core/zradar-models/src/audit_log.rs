use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AuditLog {
    pub id: i64,
    pub actor_tenant_id: Option<Uuid>,
    pub actor_project_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub metadata: serde_json::Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewAuditLog {
    pub actor_tenant_id: Option<Uuid>,
    pub actor_project_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub metadata: serde_json::Value,
}
