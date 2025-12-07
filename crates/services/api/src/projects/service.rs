//! Project service - business logic

use std::sync::Arc;
use uuid::Uuid;

use super::types::*;
use crate::audit::{AuditEvent, AuditLogger, AuditStatus};
use crate::errors::{ControlError, Result};
use crate::rbac::PermissionChecker;
use crate::users::UserRepository;

/// Project service for business operations
pub struct ProjectService {
    pub project_storage: Arc<dyn ProjectRepository>,
    pub user_storage: Arc<dyn UserRepository>,
    pub rbac: Arc<dyn PermissionChecker>,
    pub audit: Arc<dyn AuditLogger>,
}

impl ProjectService {
    /// Create a new ProjectService
    pub fn new(
        project_storage: Arc<dyn ProjectRepository>,
        user_storage: Arc<dyn UserRepository>,
        rbac: Arc<dyn PermissionChecker>,
        audit: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            project_storage,
            user_storage,
            rbac,
            audit,
        }
    }

    /// Create a new project
    pub async fn create_project(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        req: CreateProjectRequest,
    ) -> Result<Project> {
        // Check permission
        self.rbac
            .require_permission(user_id, org_id, None, "project:create")
            .await?;

        // Validate slug and name are not empty
        if req.slug.trim().is_empty() {
            return Err(ControlError::InvalidInput(
                "Project slug cannot be empty".to_string(),
            ));
        }
        if req.name.trim().is_empty() {
            return Err(ControlError::InvalidInput(
                "Project name cannot be empty".to_string(),
            ));
        }

        // Validate slug
        if !req
            .slug
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ControlError::InvalidInput(
                "Slug must contain only alphanumeric characters, hyphens, and underscores"
                    .to_string(),
            ));
        }

        // Check if slug already exists in this organization
        if self
            .project_storage
            .get_project_by_slug(org_id, &req.slug)
            .await?
            .is_some()
        {
            return Err(ControlError::Conflict(format!(
                "Project with slug '{}' already exists in this organization",
                req.slug
            )));
        }

        // Create project
        let project = self.project_storage.create_project(org_id, req).await?;

        // Log creation
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "project.created".to_string(),
                resource_type: Some("project".to_string()),
                resource_id: Some(project.id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "slug": project.slug,
                    "name": project.name
                })),
            })
            .await;

        tracing::info!(
            project_id = %project.id,
            org_id = %org_id,
            slug = %project.slug,
            user_id = %user_id,
            "Project created"
        );

        Ok(project)
    }

    /// List projects in an organization
    pub async fn list_projects(&self, user_id: Uuid, org_id: Uuid) -> Result<Vec<Project>> {
        // Check org-level read permission
        self.rbac
            .require_permission(user_id, org_id, None, "org:read")
            .await?;

        Ok(self.project_storage.list_org_projects(org_id).await?)
    }

    /// Get project by ID
    pub async fn get_project(&self, user_id: Uuid, project_id: Uuid) -> Result<Project> {
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:read",
            )
            .await?;

        Ok(project)
    }

    /// Update project
    pub async fn update_project(
        &self,
        user_id: Uuid,
        project_id: Uuid,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:settings",
            )
            .await?;

        let updated = self.project_storage.update_project(project_id, req).await?;

        // Log update
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(project.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "project.updated".to_string(),
                resource_type: Some("project".to_string()),
                resource_id: Some(project_id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::info!(project_id = %project_id, user_id = %user_id, "Project updated");

        Ok(updated)
    }

    /// Delete project
    pub async fn delete_project(&self, user_id: Uuid, project_id: Uuid) -> Result<()> {
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission (critical operation)
        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:delete",
            )
            .await?;

        self.project_storage.delete_project(project_id).await?;

        // Log deletion
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(project.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "project.deleted".to_string(),
                resource_type: Some("project".to_string()),
                resource_id: Some(project_id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::warn!(project_id = %project_id, user_id = %user_id, "Project deleted");

        Ok(())
    }

    /// List project members
    pub async fn list_members(
        &self,
        user_id: Uuid,
        project_id: Uuid,
    ) -> Result<Vec<ProjectMember>> {
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:read",
            )
            .await?;

        Ok(self.project_storage.list_members(project_id).await?)
    }

    /// Add member to project
    pub async fn add_member(
        &self,
        user_id: Uuid,
        project_id: Uuid,
        req: AddProjectMemberRequest,
    ) -> Result<ProjectMember> {
        tracing::info!(project_id = %project_id, email = %req.user_email,
                       "Adding member to project");

        // 1. Get project and check permission
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:members",
            )
            .await?;

        // 2. Look up user by email
        let target_user = self
            .user_storage
            .get_user_by_email(&req.user_email)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("User {} not found", req.user_email)))?;

        tracing::debug!(target_user_id = %target_user.id, "Found user by email");

        // 3. Check if already member
        if let Some(_existing) = self
            .project_storage
            .get_member(project_id, target_user.id)
            .await?
        {
            return Err(ControlError::Conflict(
                "User is already a project member".to_string(),
            ));
        }

        // 4. Add member
        let member = self
            .project_storage
            .add_member(project_id, target_user.id, req.clone())
            .await?;

        // 5. Audit log
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(project.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "project.member_added".to_string(),
                resource_type: Some("project_member".to_string()),
                resource_id: Some(member.id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "project_id": project_id,
                    "added_user_id": target_user.id,
                    "email": req.user_email
                })),
            })
            .await;

        tracing::info!(member_id = %member.id, "Member added successfully");
        Ok(member)
    }

    /// Remove member from project
    pub async fn remove_member(
        &self,
        user_id: Uuid,
        project_id: Uuid,
        target_user_id: Uuid,
    ) -> Result<()> {
        let project = self
            .project_storage
            .get_project(project_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac
            .require_permission(
                user_id,
                project.organization_id,
                Some(project_id),
                "project:members",
            )
            .await?;

        self.project_storage
            .remove_member(project_id, target_user_id)
            .await?;

        // Log removal
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(project.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "project.member_removed".to_string(),
                resource_type: Some("project_member".to_string()),
                resource_id: Some(target_user_id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "project_id": project_id,
                    "removed_user_id": target_user_id
                })),
            })
            .await;

        tracing::info!(
            project_id = %project_id,
            user_id = %user_id,
            target_user_id = %target_user_id,
            "Member removed from project"
        );

        Ok(())
    }
}
