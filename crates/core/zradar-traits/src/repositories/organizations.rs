//! Organization repository trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Organization entity
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct Organization {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub description: Option<String>,
    pub owner_id: Uuid,
    pub is_active: bool,
    pub plan: String,
    pub monthly_span_limit: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[sqlx(json)]
    pub settings: serde_json::Value,
    #[sqlx(json)]
    pub metadata: serde_json::Value,
}

/// Create organization request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CreateOrganizationRequest {
    pub slug: String,
    pub name: String,
    pub description: Option<String>,
}

/// Update organization request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct UpdateOrganizationRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub plan: Option<String>,
    pub monthly_span_limit: Option<i64>,
    pub settings: Option<serde_json::Value>,
}

/// Organization member
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct OrganizationMember {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Vec<String>,
    pub is_active: bool,
    pub invited_by: Option<Uuid>,
    pub joined_at: DateTime<Utc>,
}

/// Add member request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct AddMemberRequest {
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Option<Vec<String>>,
}

/// Organization with member role info
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct OrganizationWithRole {
    pub organization: Organization,
    pub member_role: Option<String>,
    pub member_permissions: Vec<String>,
}

/// Repository trait for organization persistence
#[async_trait]
pub trait OrganizationRepository: Send + Sync {
    async fn create_org(&self, owner_id: Uuid, req: CreateOrganizationRequest) -> anyhow::Result<Organization>;
    async fn get_org(&self, id: Uuid) -> anyhow::Result<Option<Organization>>;
    async fn get_org_by_slug(&self, slug: &str) -> anyhow::Result<Option<Organization>>;
    async fn list_user_orgs(&self, user_id: Uuid) -> anyhow::Result<Vec<OrganizationWithRole>>;
    async fn update_org(&self, id: Uuid, updates: UpdateOrganizationRequest) -> anyhow::Result<Organization>;
    async fn delete_org(&self, id: Uuid) -> anyhow::Result<()>;
    async fn add_member(&self, org_id: Uuid, user_id: Uuid, req: AddMemberRequest) -> anyhow::Result<OrganizationMember>;
    async fn get_member(&self, org_id: Uuid, user_id: Uuid) -> anyhow::Result<Option<OrganizationMember>>;
    async fn list_members(&self, org_id: Uuid) -> anyhow::Result<Vec<OrganizationMember>>;
    async fn remove_member(&self, org_id: Uuid, user_id: Uuid) -> anyhow::Result<()>;
}
