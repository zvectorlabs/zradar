//! Authentication bridge for OTLP services
//! 
//! Converts gRPC Request metadata to RequestContext using the
//! database-backed authentication from zradar-control.

use std::sync::Arc;
use tonic::{Request, Status};
use zradar_models::RequestContext;

// Re-export from zradar-control for external use
pub use api::auth::api_key::ApiKeyAuth as DbApiKeyAuth;

/// API Key authentication handler for gRPC services
pub struct ApiKeyAuth {
    inner: Arc<DbApiKeyAuth>,
}

impl ApiKeyAuth {
    /// Create new API key authenticator with database backend
    pub fn new(inner: Arc<DbApiKeyAuth>) -> Self {
        Self { inner }
    }
    
    /// Validate API key from gRPC request metadata
    /// 
    /// Expects Authorization header in format: `Bearer <api-key>`
    pub async fn validate<T>(&self, request: &Request<T>) -> Result<RequestContext, Status> {
        // Extract authorization header
        let metadata = request.metadata();
        let auth_header = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;
        
        // Parse "Bearer <api-key>"
        let api_key = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| {
                Status::unauthenticated("Invalid authorization format. Expected: Bearer <api-key>")
            })?;
        
        // Validate API key using database backend
        let db_context = self.inner
            .validate(api_key)
            .await
            .map_err(|e| {
                tracing::warn!("API key validation failed: {}", e);
                Status::unauthenticated("Invalid API key")
            })?;
        
        tracing::debug!(
            org_id = %db_context.organization_id,
            project_id = %db_context.project_id,
            "API key authenticated"
        );
        
        // Convert to OTLP RequestContext (uses tenant_id strings instead of UUIDs)
        Ok(RequestContext {
            tenant_id: db_context.organization_id.to_string(),
            project_id: db_context.project_id.to_string(),
            permissions: db_context.permissions,
        })
    }
}

