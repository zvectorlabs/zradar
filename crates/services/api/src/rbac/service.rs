//! RBAC service for permission checking

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::errors::{ControlError, Result};
use crate::permissions::PermissionValidator;
use crate::rbac::PermissionChecker;
use crate::domain::roles::RoleRepository;
use crate::domain::PermissionDefinition;

/// Cache entry for user permissions
#[derive(Clone)]
#[allow(dead_code)]
struct PermissionCacheEntry {
    permissions: Vec<String>,
    cached_at: std::time::Instant,
}

/// RBAC service for hierarchical permission checking
pub struct RbacService {
    storage: Arc<dyn RoleRepository>,
    validator: Arc<RwLock<Option<PermissionValidator>>>,
    #[allow(dead_code)]
    cache_ttl: std::time::Duration,
}

impl RbacService {
    pub fn new(storage: Arc<dyn RoleRepository>) -> Self {
        Self {
            storage,
            validator: Arc::new(RwLock::new(None)),
            cache_ttl: std::time::Duration::from_secs(300), // 5 minutes
        }
    }

    /// Initialize the RBAC service by loading permission definitions
    pub async fn initialize(&self) -> Result<()> {
        let definitions = self.storage.get_permission_definitions(None).await?;
        let validator = PermissionValidator::new(definitions);
        
        let mut guard = self.validator.write().await;
        *guard = Some(validator);
        
        tracing::info!("RBAC service initialized with permission definitions");
        Ok(())
    }

    /// Check if user is organization owner
    pub async fn is_org_owner(
        &self,
        user_id: Uuid,
        org_id: Uuid,
    ) -> Result<bool> {
        // Owner has all permissions
        self.check_permission(user_id, org_id, None, "*").await
    }

    /// Expand permissions for display purposes
    pub async fn expand_permissions(
        &self,
        permissions: &[String],
        scope: &str,
    ) -> Result<Vec<String>> {
        let validator_guard = self.validator.read().await;
        let validator = validator_guard
            .as_ref()
            .ok_or_else(|| ControlError::Internal("RBAC not initialized".to_string()))?;

        Ok(validator.expand_permissions(permissions, scope))
    }

    /// Validate permissions before assigning them to a role or user
    pub async fn validate_permissions(&self, permissions: &[String]) -> Result<()> {
        let validator_guard = self.validator.read().await;
        let validator = validator_guard
            .as_ref()
            .ok_or_else(|| ControlError::Internal("RBAC not initialized".to_string()))?;

        validator.validate_permissions(permissions)
    }

    /// Assess risk of a permission set
    pub async fn assess_risk(&self, permissions: &[String]) -> Result<crate::domain::RiskAssessment> {
        let validator_guard = self.validator.read().await;
        let validator = validator_guard
            .as_ref()
            .ok_or_else(|| ControlError::Internal("RBAC not initialized".to_string()))?;

        Ok(validator.assess_risk(permissions))
    }

    /// List all available permissions for a scope
    pub async fn list_permissions_by_scope(&self, scope: &str) -> Result<Vec<PermissionDefinition>> {
        Ok(self.storage.get_permission_definitions(Some(scope)).await?)
    }
}

#[async_trait]
impl PermissionChecker for RbacService {
    async fn check_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<bool> {
        let permissions = self.get_user_permissions(user_id, org_id, project_id).await?;
        
        let validator_guard = self.validator.read().await;
        let validator = validator_guard
            .as_ref()
            .ok_or_else(|| ControlError::Internal("RBAC not initialized".to_string()))?;

        let scope = if project_id.is_some() { "project" } else { "organization" };
        let has_perm = validator.has_permission(&permissions, permission, scope);

        if !has_perm {
            tracing::warn!(
                user_id = %user_id,
                org_id = %org_id,
                project_id = ?project_id,
                permission = %permission,
                "Permission check failed"
            );
        }

        Ok(has_perm)
    }

    async fn require_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<()> {
        if !self.check_permission(user_id, org_id, project_id, permission).await? {
            return Err(ControlError::Forbidden(format!(
                "Missing required permission: {}",
                permission
            )));
        }
        Ok(())
    }

    async fn get_user_permissions(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
    ) -> Result<Vec<String>> {
        let mut all_permissions = HashSet::new();

        // 1. Get organization-level permissions
        let org_perms = self.storage.get_user_org_permissions(org_id, user_id).await?;
        all_permissions.extend(org_perms);

        // 2. Get project-level permissions if project specified
        if let Some(project_id) = project_id {
            let project_perms = self.storage.get_user_project_permissions(project_id, user_id).await?;
            all_permissions.extend(project_perms);
        }

        Ok(all_permissions.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{PermissionDefinition, CustomRole, CreateCustomRoleRequest, UpdateCustomRoleRequest};
    use crate::domain::roles::RoleRepository;
    use async_trait::async_trait;
    use chrono::Utc;

    struct MockRoleStorage {
        org_permissions: Vec<String>,
        project_permissions: Vec<String>,
    }

    #[async_trait]
    impl RoleRepository for MockRoleStorage {
        async fn get_permission_definitions(&self, _scope: Option<&str>) -> anyhow::Result<Vec<PermissionDefinition>> {
            Ok(vec![
                PermissionDefinition {
                    id: "traces:read".to_string(),
                    category: "traces".to_string(),
                    action: "read".to_string(),
                    name: "View Traces".to_string(),
                    description: "View trace data".to_string(),
                    applicable_scopes: vec!["project".to_string()],
                    risk_level: "low".to_string(),
                    requires: None,
                    is_active: true,
                    created_at: Utc::now(),
                },
                PermissionDefinition {
                    id: "traces:write".to_string(),
                    category: "traces".to_string(),
                    action: "write".to_string(),
                    name: "Write Traces".to_string(),
                    description: "Send trace data".to_string(),
                    applicable_scopes: vec!["project".to_string()],
                    risk_level: "low".to_string(),
                    requires: None,
                    is_active: true,
                    created_at: Utc::now(),
                },
            ])
        }

        async fn get_permission_definition(&self, _id: &str) -> anyhow::Result<Option<PermissionDefinition>> {
            Ok(None)
        }

        async fn create_custom_role(&self, org_id: Uuid, req: CreateCustomRoleRequest, created_by: Uuid) -> anyhow::Result<CustomRole> {
            Ok(CustomRole {
                id: Uuid::new_v4(),
                organization_id: org_id,
                name: req.name,
                description: req.description,
                scope: req.scope,
                permissions: req.permissions,
                is_system: false,
                color: req.color,
                created_by: Some(created_by),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
        }

        async fn get_custom_role(&self, _id: Uuid) -> anyhow::Result<Option<CustomRole>> {
            Ok(None)
        }

        async fn list_custom_roles(&self, _org_id: Uuid, _scope: Option<&str>) -> anyhow::Result<Vec<CustomRole>> {
            Ok(vec![])
        }

        async fn update_custom_role(&self, id: Uuid, updates: UpdateCustomRoleRequest) -> anyhow::Result<CustomRole> {
            Ok(CustomRole {
                id,
                organization_id: Uuid::new_v4(),
                name: "Updated Role".to_string(),
                description: updates.description,
                scope: "organization".to_string(),
                permissions: updates.permissions.unwrap_or_default(),
                is_system: false,
                color: updates.color,
                created_by: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
        }

        async fn delete_custom_role(&self, _id: Uuid) -> anyhow::Result<()> {
            Ok(())
        }

        async fn get_user_org_permissions(&self, _org_id: Uuid, _user_id: Uuid) -> anyhow::Result<Vec<String>> {
            Ok(self.org_permissions.clone())
        }

        async fn get_user_project_permissions(&self, _project_id: Uuid, _user_id: Uuid) -> anyhow::Result<Vec<String>> {
            Ok(self.project_permissions.clone())
        }
    }

    #[tokio::test]
    async fn test_check_permission_with_wildcard() {
        let storage = Arc::new(MockRoleStorage {
            org_permissions: vec!["traces:*".to_string()],
            project_permissions: vec![],
        });

        let rbac = RbacService::new(storage);
        rbac.initialize().await.unwrap();

        let user_id = Uuid::new_v4();
        let org_id = Uuid::new_v4();
        let project_id = Some(Uuid::new_v4());

        assert!(rbac.check_permission(user_id, org_id, project_id, "traces:read").await.unwrap());
        assert!(rbac.check_permission(user_id, org_id, project_id, "traces:write").await.unwrap());
    }

    #[tokio::test]
    async fn test_hierarchical_permissions() {
        let storage = Arc::new(MockRoleStorage {
            org_permissions: vec!["traces:read".to_string()],
            project_permissions: vec!["traces:write".to_string()],
        });

        let rbac = RbacService::new(storage);
        rbac.initialize().await.unwrap();

        let user_id = Uuid::new_v4();
        let org_id = Uuid::new_v4();
        let project_id = Some(Uuid::new_v4());

        // Should have both org and project permissions
        let perms = rbac.get_user_permissions(user_id, org_id, project_id).await.unwrap();
        assert!(perms.contains(&"traces:read".to_string()));
        assert!(perms.contains(&"traces:write".to_string()));
    }
}

