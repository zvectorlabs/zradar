//! Policy administration service trait.
//!
//! Abstracts CRUD operations on ingestion/query policy limits so that both
//! HTTP and gRPC transports can share the same business logic.

use async_trait::async_trait;
use zradar_models::WorkspaceId;

use crate::errors::ServiceError;

/// Policy administration service trait.
///
/// Manages workspace-level policy limits (ingestion quotas, query rate limits,
/// storage caps, etc.).
#[async_trait]
pub trait PolicyAdminService: Send + Sync {
    /// Get all policy limits for a workspace, serialized as JSON value.
    async fn get_policies(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<serde_json::Value, ServiceError>;

    /// Upsert a policy limit for a workspace.
    async fn upsert_policy(
        &self,
        workspace_id: WorkspaceId,
        policy: serde_json::Value,
    ) -> Result<serde_json::Value, ServiceError>;

    /// Delete a specific policy limit.
    async fn delete_policy(
        &self,
        workspace_id: WorkspaceId,
        policy_id: &str,
    ) -> Result<(), ServiceError>;
}
