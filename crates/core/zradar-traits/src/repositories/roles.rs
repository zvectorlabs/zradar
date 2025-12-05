//! Role repository trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Permission definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct PermissionDefinition {
    pub id: String,
    pub category: String,
    pub action: String,
    pub name: String,
    pub description: String,
    pub applicable_scopes: Vec<String>,
    pub risk_level: String,
    pub requires: Option<Vec<String>>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

/// Custom role defined by an organization
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CustomRole {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub scope: String,  // 'organization' or 'project'
    pub permissions: Vec<String>,
    pub is_system: bool,
    pub color: Option<String>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request to create a custom role
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CreateCustomRoleRequest {
    pub name: String,
    pub description: Option<String>,
    pub scope: String,
    pub permissions: Vec<String>,
    pub color: Option<String>,
}

/// Request to update a custom role
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct UpdateCustomRoleRequest {
    pub description: Option<String>,
    pub permissions: Option<Vec<String>>,
    pub color: Option<String>,
}

/// Risk assessment for permissions
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct RiskAssessment {
    pub has_high_risk: bool,
    pub high_risk_permissions: Vec<PermissionInfo>,
}

/// Permission info for risk assessment
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct PermissionInfo {
    pub id: String,
    pub name: String,
    pub risk_level: String,
}

/// Repository trait for role and permission persistence
#[async_trait]
pub trait RoleRepository: Send + Sync {
    /// Get permission definitions
    async fn get_permission_definitions(&self, scope: Option<&str>) -> anyhow::Result<Vec<PermissionDefinition>>;
    
    /// Get a single permission definition by ID
    async fn get_permission_definition(&self, id: &str) -> anyhow::Result<Option<PermissionDefinition>>;
    
    /// Create a custom role
    async fn create_custom_role(&self, org_id: Uuid, req: CreateCustomRoleRequest, created_by: Uuid) -> anyhow::Result<CustomRole>;
    
    /// Get a custom role by ID
    async fn get_custom_role(&self, id: Uuid) -> anyhow::Result<Option<CustomRole>>;
    
    /// List custom roles for an organization
    async fn list_custom_roles(&self, org_id: Uuid, scope: Option<&str>) -> anyhow::Result<Vec<CustomRole>>;
    
    /// Update a custom role
    async fn update_custom_role(&self, id: Uuid, updates: UpdateCustomRoleRequest) -> anyhow::Result<CustomRole>;
    
    /// Delete a custom role
    async fn delete_custom_role(&self, id: Uuid) -> anyhow::Result<()>;
    
    /// Get user permissions at organization level
    async fn get_user_org_permissions(&self, org_id: Uuid, user_id: Uuid) -> anyhow::Result<Vec<String>>;
    
    /// Get user permissions at project level
    async fn get_user_project_permissions(&self, project_id: Uuid, user_id: Uuid) -> anyhow::Result<Vec<String>>;
}
