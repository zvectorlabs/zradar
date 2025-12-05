//! Mock permission checker for unit testing

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

use crate::errors::{ControlError, Result};
use super::permission_checker::PermissionChecker;

type PermissionKey = (Uuid, Uuid, Option<Uuid>); // (user_id, org_id, project_id)

/// Mock permission checker for testing
pub struct MockPermissionChecker {
    permissions: Mutex<HashMap<PermissionKey, Vec<String>>>,
    always_allow: bool,
}

impl Default for MockPermissionChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPermissionChecker {
    pub fn new() -> Self {
        Self {
            permissions: Mutex::new(HashMap::new()),
            always_allow: false,
        }
    }

    /// Create a mock that always allows all permissions
    pub fn always_allow() -> Self {
        Self {
            permissions: Mutex::new(HashMap::new()),
            always_allow: true,
        }
    }

    /// Grant permissions to a user
    pub fn grant_permissions(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permissions: Vec<String>,
    ) {
        let key = (user_id, org_id, project_id);
        self.permissions.lock().unwrap().insert(key, permissions);
    }

    /// Clear all permissions
    pub fn clear(&self) {
        self.permissions.lock().unwrap().clear();
    }
}

#[async_trait]
impl PermissionChecker for MockPermissionChecker {
    async fn check_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<bool> {
        if self.always_allow {
            return Ok(true);
        }

        let key = (user_id, org_id, project_id);
        let permissions = self.permissions.lock().unwrap();
        
        if let Some(user_perms) = permissions.get(&key) {
            // Check for exact permission or wildcard
            Ok(user_perms.contains(&permission.to_string())
                || user_perms.contains(&"*".to_string())
                || user_perms.iter().any(|p| {
                    p.ends_with(":*") && permission.starts_with(&p[..p.len() - 1])
                }))
        } else {
            Ok(false)
        }
    }

    async fn require_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<()> {
        if self.check_permission(user_id, org_id, project_id, permission).await? {
            Ok(())
        } else {
            Err(ControlError::Forbidden(format!(
                "Missing required permission: {}",
                permission
            )))
        }
    }

    async fn get_user_permissions(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
    ) -> Result<Vec<String>> {
        if self.always_allow {
            return Ok(vec!["*".to_string()]);
        }

        let key = (user_id, org_id, project_id);
        let permissions = self.permissions.lock().unwrap();
        
        Ok(permissions.get(&key).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_permission_checker() {
        let checker = MockPermissionChecker::new();
        
        let user_id = Uuid::new_v4();
        let org_id = Uuid::new_v4();
        let project_id = Some(Uuid::new_v4());

        // Initially no permissions
        assert!(!checker.check_permission(user_id, org_id, project_id, "traces:read").await.unwrap());

        // Grant permissions
        checker.grant_permissions(
            user_id,
            org_id,
            project_id,
            vec!["traces:read".to_string(), "traces:write".to_string()],
        );

        // Check permissions
        assert!(checker.check_permission(user_id, org_id, project_id, "traces:read").await.unwrap());
        assert!(checker.check_permission(user_id, org_id, project_id, "traces:write").await.unwrap());
        assert!(!checker.check_permission(user_id, org_id, project_id, "traces:delete").await.unwrap());

        // Wildcard permissions
        checker.grant_permissions(
            user_id,
            org_id,
            None,
            vec!["*".to_string()],
        );
        assert!(checker.check_permission(user_id, org_id, None, "any:permission").await.unwrap());
    }

    #[tokio::test]
    async fn test_always_allow() {
        let checker = MockPermissionChecker::always_allow();
        
        let user_id = Uuid::new_v4();
        let org_id = Uuid::new_v4();

        assert!(checker.check_permission(user_id, org_id, None, "any:permission").await.unwrap());
        assert!(checker.require_permission(user_id, org_id, None, "any:permission").await.is_ok());
    }
}

