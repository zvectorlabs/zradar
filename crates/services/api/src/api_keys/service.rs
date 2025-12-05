//! API Key service - use case orchestration

use std::sync::Arc;
use uuid::Uuid;

use super::types::{
    ApiKeyResponse, CreateApiKeyRequest, CreateApiKeyResponse,
    ApiKeyRepository,
};
use crate::projects::ProjectRepository;
use crate::audit::{AuditLogger, AuditEvent, AuditStatus};
use crate::rbac::PermissionChecker;
use crate::errors::{ControlError, Result};

/// API Key validator trait
#[async_trait::async_trait]
pub trait ApiKeyValidator: Send + Sync {
    async fn revoke(&self, key_id: Uuid) -> Result<()>;
}

/// Key generation utilities (static methods)
pub trait KeyGenerator {
    fn generate_key(prefix: &str) -> String;
    fn hash_key(key: &str) -> String;
}

/// API Key service for business operations
pub struct ApiKeyService<G: KeyGenerator> {
    pub api_key_storage: Arc<dyn ApiKeyRepository>,
    pub project_storage: Arc<dyn ProjectRepository>,
    pub rbac: Arc<dyn PermissionChecker>,
    pub audit: Arc<dyn AuditLogger>,
    pub auth: Arc<dyn ApiKeyValidator>,
    _marker: std::marker::PhantomData<G>,
}

impl<G: KeyGenerator> ApiKeyService<G> {
    /// Create a new ApiKeyService
    pub fn new(
        api_key_storage: Arc<dyn ApiKeyRepository>,
        project_storage: Arc<dyn ProjectRepository>,
        rbac: Arc<dyn PermissionChecker>,
        audit: Arc<dyn AuditLogger>,
        auth: Arc<dyn ApiKeyValidator>,
    ) -> Self {
        Self {
            api_key_storage,
            project_storage,
            rbac,
            audit,
            auth,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new API key
    pub async fn create_api_key(
        &self,
        user_id: Uuid,
        project_id: Uuid,
        req: CreateApiKeyRequest,
    ) -> Result<CreateApiKeyResponse> {
        // Get project
        let project = self.project_storage.get_project(project_id).await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac.require_permission(
            user_id,
            project.organization_id,
            Some(project_id),
            "api_keys:create"
        ).await?;

        // Generate API key
        let key = G::generate_key("zvr_live");
        let key_hash = G::hash_key(&key);
        let key_prefix = key.chars().take(12).collect::<String>();

        // Create API key in database
        let api_key = self.api_key_storage.create_key(
            project.organization_id,
            project_id,
            key_hash,
            key_prefix.clone(),
            req.clone(),
            user_id,
        ).await?;

        // Log creation
        let _ = self.audit.log(AuditEvent {
            organization_id: Some(project.organization_id),
            user_id: Some(user_id),
            actor_type: Some("user".to_string()),
            actor_id: Some(user_id),
            actor_ip: None,
            action: "api_key.created".to_string(),
            resource_type: Some("api_key".to_string()),
            resource_id: Some(api_key.id),
            status: AuditStatus::Success,
            details: Some(serde_json::json!({
                "key_id": api_key.id,
                "key_prefix": key_prefix,
                "project_id": project_id,
                "permissions": api_key.permissions
            })),
        }).await;

        tracing::info!(
            key_id = %api_key.id,
            project_id = %project_id,
            user_id = %user_id,
            "API key created"
        );

        Ok(CreateApiKeyResponse {
            id: api_key.id,
            key,  // Return the actual key (only time it's shown)
            key_prefix,
            name: api_key.name,
            permissions: api_key.permissions,
            expires_at: api_key.expires_at,
            created_at: api_key.created_at,
        })
    }

    /// List API keys for a project
    pub async fn list_api_keys(
        &self,
        user_id: Uuid,
        project_id: Uuid,
    ) -> Result<Vec<ApiKeyResponse>> {
        // Get project
        let project = self.project_storage.get_project(project_id).await?
            .ok_or_else(|| ControlError::NotFound("Project not found".to_string()))?;

        // Check permission
        self.rbac.require_permission(
            user_id,
            project.organization_id,
            Some(project_id),
            "api_keys:read"
        ).await?;

        let keys = self.api_key_storage.list_keys(project.organization_id, project_id).await?;
        let response: Vec<ApiKeyResponse> = keys.into_iter().map(|k| k.into()).collect();

        Ok(response)
    }

    /// Get API key details
    pub async fn get_api_key(
        &self,
        user_id: Uuid,
        key_id: Uuid,
    ) -> Result<ApiKeyResponse> {
        let api_key = self.api_key_storage.get_key(key_id).await?
            .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

        // Check permission
        self.rbac.require_permission(
            user_id,
            api_key.organization_id,
            Some(api_key.project_id),
            "api_keys:read"
        ).await?;

        Ok(api_key.into())
    }

    /// Revoke an API key
    pub async fn revoke_api_key(
        &self,
        user_id: Uuid,
        key_id: Uuid,
    ) -> Result<()> {
        let api_key = self.api_key_storage.get_key(key_id).await?
            .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

        // Check permission
        self.rbac.require_permission(
            user_id,
            api_key.organization_id,
            Some(api_key.project_id),
            "api_keys:revoke"
        ).await?;

        self.api_key_storage.revoke_key(key_id).await?;

        // Invalidate cache to ensure revoked key is rejected immediately
        self.auth.revoke(key_id).await?;

        // Log revocation
        let _ = self.audit.log(AuditEvent {
            organization_id: Some(api_key.organization_id),
            user_id: Some(user_id),
            actor_type: Some("user".to_string()),
            actor_id: Some(user_id),
            actor_ip: None,
            action: "api_key.revoked".to_string(),
            resource_type: Some("api_key".to_string()),
            resource_id: Some(key_id),
            status: AuditStatus::Success,
            details: Some(serde_json::json!({
                "key_id": key_id,
                "key_prefix": api_key.key_prefix
            })),
        }).await;

        tracing::info!(key_id = %key_id, user_id = %user_id, "API key revoked");

        Ok(())
    }

    /// Delete an API key
    pub async fn delete_api_key(
        &self,
        user_id: Uuid,
        key_id: Uuid,
    ) -> Result<()> {
        let api_key = self.api_key_storage.get_key(key_id).await?
            .ok_or_else(|| ControlError::NotFound("API key not found".to_string()))?;

        // Check permission (high-risk operation)
        self.rbac.require_permission(
            user_id,
            api_key.organization_id,
            Some(api_key.project_id),
            "api_keys:delete"
        ).await?;

        self.api_key_storage.delete_key(key_id).await?;

        // Invalidate cache
        self.auth.revoke(key_id).await?;

        // Log deletion
        let _ = self.audit.log(AuditEvent {
            organization_id: Some(api_key.organization_id),
            user_id: Some(user_id),
            actor_type: Some("user".to_string()),
            actor_id: Some(user_id),
            actor_ip: None,
            action: "api_key.deleted".to_string(),
            resource_type: Some("api_key".to_string()),
            resource_id: Some(key_id),
            status: AuditStatus::Success,
            details: Some(serde_json::json!({
                "key_id": key_id,
                "key_prefix": api_key.key_prefix
            })),
        }).await;

        tracing::warn!(key_id = %key_id, user_id = %user_id, "API key deleted");

        Ok(())
    }
}

