//! Project repository trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Project within an organization
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct Project {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub slug: String,
    pub name: String,
    pub description: Option<String>,
    pub environment: String,
    pub is_active: bool,
    pub retention_days: i32,
    pub sampling_rate: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub settings: serde_json::Value,
    pub metadata: serde_json::Value,
}

/// Request to create a project
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct CreateProjectRequest {
    pub slug: String,
    pub name: String,
    pub description: Option<String>,
    pub environment: Option<String>,
    pub retention_days: Option<i32>,
    pub sampling_rate: Option<f64>,
}

/// Request to update a project
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct UpdateProjectRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub environment: Option<String>,
    pub retention_days: Option<i32>,
    pub sampling_rate: Option<f64>,
    pub settings: Option<serde_json::Value>,
}

/// Project member
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct ProjectMember {
    pub id: Uuid,
    pub project_id: Uuid,
    pub user_id: Uuid,
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Vec<String>,
    pub is_active: bool,
    pub added_by: Option<Uuid>,
    pub joined_at: DateTime<Utc>,
}

/// Request to add a member to a project
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct AddProjectMemberRequest {
    pub user_email: String,
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Option<Vec<String>>,
}

/// Project with member info
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct ProjectWithRole {
    #[serde(flatten)]
    pub project: Project,
    pub member_role: Option<String>,
    pub member_permissions: Vec<String>,
}

/// Repository trait for project persistence
#[async_trait]
pub trait ProjectRepository: Send + Sync {
    /// Create a new project
    async fn create_project(
        &self,
        org_id: Uuid,
        req: CreateProjectRequest,
    ) -> anyhow::Result<Project>;

    /// Get project by ID
    async fn get_project(&self, id: Uuid) -> anyhow::Result<Option<Project>>;

    /// Get project by slug within an organization
    async fn get_project_by_slug(
        &self,
        org_id: Uuid,
        slug: &str,
    ) -> anyhow::Result<Option<Project>>;

    /// List all projects in an organization
    async fn list_org_projects(&self, org_id: Uuid) -> anyhow::Result<Vec<Project>>;

    /// List projects accessible to a user within an organization
    async fn list_user_projects(
        &self,
        org_id: Uuid,
        user_id: Uuid,
    ) -> anyhow::Result<Vec<ProjectWithRole>>;

    /// Update a project
    async fn update_project(
        &self,
        id: Uuid,
        updates: UpdateProjectRequest,
    ) -> anyhow::Result<Project>;

    /// Delete a project
    async fn delete_project(&self, id: Uuid) -> anyhow::Result<()>;

    // Member operations

    /// Add a member to a project
    async fn add_member(
        &self,
        project_id: Uuid,
        user_id: Uuid,
        req: AddProjectMemberRequest,
    ) -> anyhow::Result<ProjectMember>;

    /// Get a member by project and user ID
    async fn get_member(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> anyhow::Result<Option<ProjectMember>>;

    /// List all members of a project
    async fn list_members(&self, project_id: Uuid) -> anyhow::Result<Vec<ProjectMember>>;

    /// Remove a member from a project
    async fn remove_member(&self, project_id: Uuid, user_id: Uuid) -> anyhow::Result<()>;
}
