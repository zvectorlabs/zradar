//! Role service - use case orchestration

use std::sync::Arc;
use uuid::Uuid;

use super::types::{
    CreateCustomRoleRequest, CustomRole, PermissionDefinition, RoleRepository,
    UpdateCustomRoleRequest,
};
use crate::audit::{AuditEvent, AuditLogger, AuditStatus};
use crate::errors::{ControlError, Result};
use crate::rbac::PermissionChecker;

/// Role service for business operations
pub struct RoleService {
    pub role_storage: Arc<dyn RoleRepository>,
    pub rbac: Arc<dyn PermissionChecker>,
    pub audit: Arc<dyn AuditLogger>,
}

impl RoleService {
    /// Create a new RoleService
    pub fn new(
        role_storage: Arc<dyn RoleRepository>,
        rbac: Arc<dyn PermissionChecker>,
        audit: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            role_storage,
            rbac,
            audit,
        }
    }

    /// Create a new custom role
    pub async fn create_role(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        req: CreateCustomRoleRequest,
    ) -> Result<CustomRole> {
        tracing::info!("Creating custom role: {} in org {}", req.name, org_id);

        // Check permission
        self.rbac
            .check_permission(user_id, org_id, None, "admin:roles")
            .await?;

        // Basic permission validation (ensure not empty)
        if req.permissions.is_empty() {
            return Err(ControlError::InvalidInput(
                "Role must have at least one permission".to_string(),
            ));
        }

        // Create role
        let role = self
            .role_storage
            .create_custom_role(org_id, req.clone(), user_id)
            .await?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: Some(org_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "create".to_string(),
                resource_type: Some("custom_role".to_string()),
                resource_id: Some(role.id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "role_name": role.name,
                    "permissions": role.permissions,
                })),
            })
            .await?;

        tracing::info!("Created custom role: {} ({})", role.name, role.id);

        Ok(role)
    }

    /// List custom roles for an organization
    pub async fn list_roles(&self, user_id: Uuid, org_id: Uuid) -> Result<Vec<CustomRole>> {
        // Check permission
        self.rbac
            .check_permission(user_id, org_id, None, "read:roles")
            .await?;

        Ok(self.role_storage.list_custom_roles(org_id, None).await?)
    }

    /// Get a specific custom role
    pub async fn get_role(&self, user_id: Uuid, role_id: Uuid) -> Result<CustomRole> {
        let role = self
            .role_storage
            .get_custom_role(role_id)
            .await?
            .ok_or(ControlError::NotFound("Role not found".to_string()))?;

        // Check permission
        self.rbac
            .check_permission(user_id, role.organization_id, None, "read:roles")
            .await?;

        Ok(role)
    }

    /// Update a custom role
    pub async fn update_role(
        &self,
        user_id: Uuid,
        role_id: Uuid,
        req: UpdateCustomRoleRequest,
    ) -> Result<CustomRole> {
        let existing_role = self
            .role_storage
            .get_custom_role(role_id)
            .await?
            .ok_or(ControlError::NotFound("Role not found".to_string()))?;

        // Check permission
        self.rbac
            .check_permission(user_id, existing_role.organization_id, None, "admin:roles")
            .await?;

        // Validate new permissions if provided
        if let Some(ref perms) = req.permissions
            && perms.is_empty()
        {
            return Err(ControlError::InvalidInput(
                "Role must have at least one permission".to_string(),
            ));
        }

        // Update role
        let updated_role = self
            .role_storage
            .update_custom_role(role_id, req.clone())
            .await?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: Some(existing_role.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "update".to_string(),
                resource_type: Some("custom_role".to_string()),
                resource_id: Some(role_id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "changes": req,
                })),
            })
            .await?;

        tracing::info!("Updated custom role: {}", role_id);

        Ok(updated_role)
    }

    /// Delete a custom role
    pub async fn delete_role(&self, user_id: Uuid, role_id: Uuid) -> Result<()> {
        let role = self
            .role_storage
            .get_custom_role(role_id)
            .await?
            .ok_or(ControlError::NotFound("Role not found".to_string()))?;

        // Check permission
        self.rbac
            .check_permission(user_id, role.organization_id, None, "admin:roles")
            .await?;

        // Delete role
        self.role_storage.delete_custom_role(role_id).await?;

        // Audit log
        self.audit
            .log(AuditEvent {
                organization_id: Some(role.organization_id),
                user_id: Some(user_id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user_id),
                actor_ip: None,
                action: "delete".to_string(),
                resource_type: Some("custom_role".to_string()),
                resource_id: Some(role_id),
                status: AuditStatus::Success,
                details: Some(serde_json::json!({
                    "role_name": role.name,
                })),
            })
            .await?;

        tracing::info!("Deleted custom role: {}", role_id);

        Ok(())
    }

    /// List all available permission definitions
    pub async fn list_permissions(&self) -> Result<Vec<PermissionDefinition>> {
        Ok(self.role_storage.get_permission_definitions(None).await?)
    }
}
