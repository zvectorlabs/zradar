//! API key validation trait

use async_trait::async_trait;
use uuid::Uuid;

use crate::auth::RequestContext;
use crate::errors::Result;

/// Trait for API key validation
#[async_trait]
pub trait ApiKeyValidator: Send + Sync {
    /// Validate an API key and return request context
    async fn validate(&self, key: &str) -> Result<RequestContext>;
    
    /// Revoke an API key (invalidate cache)
    async fn revoke(&self, key_id: Uuid) -> Result<()>;
}

