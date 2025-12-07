//! Organization service - business logic

use std::sync::Arc;
use uuid::Uuid;

use super::types::*;
use crate::audit::{AuditEvent, AuditLogger, AuditStatus};
use crate::errors::{ControlError, Result};
use crate::rbac::PermissionChecker;
use zradar_traits::UserRepository;

/// Organization service for business operations
pub struct OrganizationService {
    pub org_storage: Arc<dyn OrganizationRepository>,
    pub user_storage: Arc<dyn UserRepository>,
    pub rbac: Arc<dyn PermissionChecker>,
    pub audit: Arc<dyn AuditLogger>,
}

impl OrganizationService {
    /// Create a new OrganizationService
    pub fn new(
        org_storage: Arc<dyn OrganizationRepository>,
        user_storage: Arc<dyn UserRepository>,
        rbac: Arc<dyn PermissionChecker>,
        audit: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            org_storage,
            user_storage,
            rbac,
            audit,
        }
    }

    /// Create a new organization
    pub async fn create_organization(
        &self,
        user_id: Uuid,
        req: CreateOrganizationRequest,
    ) -> Result<Organization> {
        // Validate slug and name are not empty
        if req.slug.trim().is_empty() {
            return Err(ControlError::InvalidInput(
                "Organization slug cannot be empty".to_string(),
            ));
        }
        if req.name.trim().is_empty() {
            return Err(ControlError::InvalidInput(
                "Organization name cannot be empty".to_string(),
            ));
        }

        // Validate slug format
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

        // Check if slug already exists
        if self.org_storage.get_org_by_slug(&req.slug).await?.is_some() {
            return Err(ControlError::Conflict(format!(
                "Organization with slug '{}' already exists",
                req.slug
            )));
        }

        // Create organization
        let org = self.org_storage.create_org(user_id, req).await?;

        // Log creation
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(org.id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "organization.created".to_string(),
                resource_type: Some("organization".to_string()),
                resource_id: Some(org.id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "slug": org.slug,
                    "name": org.name
                })),
            })
            .await;

        tracing::info!(org_id = %org.id, slug = %org.slug, user_id = %user_id, "Organization created");

        Ok(org)
    }

    /// List user's organizations
    pub async fn list_user_organizations(
        &self,
        user_id: Uuid,
    ) -> Result<Vec<OrganizationWithRole>> {
        Ok(self.org_storage.list_user_orgs(user_id).await?)
    }

    /// Get organization by ID with permission check
    pub async fn get_organization(&self, user_id: Uuid, org_id: Uuid) -> Result<Organization> {
        // Check if user has access to this organization
        self.rbac
            .require_permission(user_id, org_id, None, "org:read")
            .await?;

        self.org_storage
            .get_org(org_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("Organization not found".to_string()))
    }

    /// Update organization
    pub async fn update_organization(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        req: UpdateOrganizationRequest,
    ) -> Result<Organization> {
        // Check permission
        self.rbac
            .require_permission(user_id, org_id, None, "org:settings")
            .await?;

        let org = self.org_storage.update_org(org_id, req).await?;

        // Log update
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "organization.updated".to_string(),
                resource_type: Some("organization".to_string()),
                resource_id: Some(org_id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::info!(org_id = %org_id, user_id = %user_id, "Organization updated");

        Ok(org)
    }

    /// Delete organization
    pub async fn delete_organization(&self, user_id: Uuid, org_id: Uuid) -> Result<()> {
        // Check permission (requires critical permission)
        self.rbac
            .require_permission(user_id, org_id, None, "org:delete")
            .await?;

        self.org_storage.delete_org(org_id).await?;

        // Log deletion
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "organization.deleted".to_string(),
                resource_type: Some("organization".to_string()),
                resource_id: Some(org_id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::warn!(org_id = %org_id, user_id = %user_id, "Organization deleted");

        Ok(())
    }

    /// List organization members
    pub async fn list_members(
        &self,
        user_id: Uuid,
        org_id: Uuid,
    ) -> Result<Vec<OrganizationMemberResponse>> {
        // Check permission
        self.rbac
            .require_permission(user_id, org_id, None, "org:read")
            .await?;

        let members = self.org_storage.list_members(org_id).await?;

        let mut response = Vec::new();
        for member in members {
            let user = self
                .user_storage
                .get_user(member.user_id)
                .await
                .ok()
                .flatten();
            let mut resp = OrganizationMemberResponse::from(member);
            if let Some(u) = user {
                resp.user_email = Some(u.email);
                resp.user_full_name = u.full_name;
            }
            response.push(resp);
        }

        Ok(response)
    }

    /// Add member to organization
    pub async fn add_member(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        req: AddOrganizationMemberRequest,
    ) -> Result<OrganizationMember> {
        // Check permission
        self.rbac
            .check_permission(user_id, org_id, None, "members:write")
            .await?;

        // Look up user by email
        let target_user = self
            .user_storage
            .get_user_by_email(&req.user_email)
            .await
            .map_err(|e| ControlError::Internal(format!("Failed to lookup user: {}", e)))?
            .ok_or_else(|| {
                ControlError::NotFound(format!("User with email {} not found", req.user_email))
            })?;

        // Convert request to AddMemberRequest (zradar-traits type)
        let add_req = zradar_traits::AddMemberRequest {
            role: req.role,
            custom_role_id: req.custom_role_id,
            permissions: None, // Use default permissions from role
        };

        // Add member
        let member = self
            .org_storage
            .add_member(org_id, target_user.id, add_req)
            .await
            .map_err(|e| ControlError::Internal(format!("Failed to add member: {}", e)))?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "organization.member.add".to_string(),
                resource_type: Some("organization_member".to_string()),
                resource_id: Some(member.id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "target_user_id": target_user.id,
                    "role": member.role,
                })),
            })
            .await
            .ok();

        Ok(member)
    }

    /// Remove member from organization
    pub async fn remove_member(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        target_user_id: Uuid,
    ) -> Result<()> {
        // Check permission
        self.rbac
            .require_permission(user_id, org_id, None, "org:members")
            .await?;

        self.org_storage
            .remove_member(org_id, target_user_id)
            .await?;

        // Log removal
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "organization.member_removed".to_string(),
                resource_type: Some("organization_member".to_string()),
                resource_id: Some(target_user_id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({ "removed_user_id": target_user_id })),
            })
            .await;

        tracing::info!(org_id = %org_id, user_id = %user_id, target_user_id = %target_user_id, "Member removed from organization");

        Ok(())
    }
}
