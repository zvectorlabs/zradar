//! gRPC handler for the `AuditService` RPC (audit log queries).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_traits::{AdminAuthorizer, AuditLogFilters, Capability};

use crate::audit::handlers::AuditState;

use super::admin_proto::audit_service_server::AuditService as AuditServiceRpc;
use super::admin_proto::*;
use super::auth::authorize_admin;
use super::conversions::{audit_log_to_proto, map_anyhow_error};

/// Tonic handler that delegates to [`AuditState`].
pub struct AuditHandler {
    state: Arc<AuditState>,
    auth: Arc<dyn AdminAuthorizer>,
}

impl AuditHandler {
    pub fn new(state: Arc<AuditState>, auth: Arc<dyn AdminAuthorizer>) -> Self {
        Self { state, auth }
    }
}

#[tonic::async_trait]
impl AuditServiceRpc for AuditHandler {
    async fn list_audit_logs(
        &self,
        request: Request<ListAuditLogsRequest>,
    ) -> Result<Response<ListAuditLogsResponse>, Status> {
        let (req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();

        let page = self
            .state
            .repository
            .list_audit_logs(AuditLogFilters {
                workspace_id: Some(workspace_id.into_inner()),
                action: req.action,
                resource_type: req.resource_type,
                resource_id: req.resource_id,
                start_created_at: req.start_created_at,
                end_created_at: req.end_created_at,
                limit: req.limit,
                offset: req.offset,
            })
            .await
            .map_err(map_anyhow_error)?;

        Ok(Response::new(ListAuditLogsResponse {
            items: page.items.iter().map(audit_log_to_proto).collect(),
            total: page.total,
            limit: page.limit,
            offset: page.offset,
        }))
    }
}
