//! Retention service trait.
//!
//! Abstracts retention-policy enforcement and cleanup operations so that
//! transport layers can trigger retention jobs through a common interface.

use async_trait::async_trait;
use zradar_models::WorkspaceId;

use crate::errors::ServiceError;

/// Retention management service trait.
#[async_trait]
pub trait RetentionService: Send + Sync {
    /// Run a retention cleanup cycle for the given workspace.
    ///
    /// Deletes telemetry data older than the configured retention window and
    /// records storage-usage accounting deltas.
    async fn run_cleanup(&self, workspace_id: WorkspaceId) -> Result<u64, ServiceError>;

    /// Run retention cleanup for all workspaces.
    async fn run_cleanup_all(&self) -> Result<u64, ServiceError>;
}
