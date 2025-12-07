//! Permission checking trait

use async_trait::async_trait;
use uuid::Uuid;

use crate::errors::Result;

/// Trait for checking permissions
#[async_trait]
pub trait PermissionChecker: Send + Sync {
    /// Check if user has a specific permission in an organization or project
    async fn check_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<bool>;

    /// Require a permission or return an error
    async fn require_permission(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
        permission: &str,
    ) -> Result<()>;

    /// Get all effective permissions for a user at a given scope
    async fn get_user_permissions(
        &self,
        user_id: Uuid,
        org_id: Uuid,
        project_id: Option<Uuid>,
    ) -> Result<Vec<String>>;
}
