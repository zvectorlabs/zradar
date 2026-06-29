//! gRPC handler for the `PolicyService` RPC (policy administration).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_policy::{Operation, SignalKind};
use zradar_traits::{AdminAuthorizer, Capability};

use crate::policy::handlers::PolicyState;

use super::admin_proto::policy_service_server::PolicyService as PolicyServiceRpc;
use super::admin_proto::*;
use super::auth::authorize_admin;
use super::conversions::{policy_config_to_policy, policy_to_proto, resolved_policy_to_proto};

/// Tonic handler that delegates to [`PolicyState`].
pub struct PolicyHandler {
    state: Arc<PolicyState>,
    auth: Arc<dyn AdminAuthorizer>,
}

impl PolicyHandler {
    pub fn new(state: Arc<PolicyState>, auth: Arc<dyn AdminAuthorizer>) -> Self {
        Self { state, auth }
    }
}

#[tonic::async_trait]
impl PolicyServiceRpc for PolicyHandler {
    async fn list_policies(
        &self,
        request: Request<ListPoliciesRequest>,
    ) -> Result<Response<ListPoliciesResponse>, Status> {
        let (_req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();

        let policies = self
            .state
            .store
            .list(workspace_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ListPoliciesResponse {
            policies: policies.iter().map(policy_to_proto).collect(),
        }))
    }

    async fn upsert_policies(
        &self,
        request: Request<UpsertPoliciesRequest>,
    ) -> Result<Response<UpsertPoliciesResponse>, Status> {
        let (req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();

        let policies: Result<Vec<_>, Status> = req
            .policies
            .iter()
            .map(|cfg| policy_config_to_policy(workspace_id, cfg))
            .collect();
        let policies = policies?;

        self.state
            .store
            .upsert_many(policies)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        Ok(Response::new(UpsertPoliciesResponse {}))
    }

    async fn get_effective_policy(
        &self,
        request: Request<GetEffectivePolicyRequest>,
    ) -> Result<Response<GetEffectivePolicyResponse>, Status> {
        let (_req, auth) = authorize_admin(&self.auth, request, Capability::Admin).await?;
        let workspace_id = auth.workspace_id();

        Ok(Response::new(GetEffectivePolicyResponse {
            ingest: Some(resolved_policy_to_proto(&self.state.store.resolve(
                workspace_id,
                SignalKind::All,
                Operation::Ingest,
            ))),
            query: Some(resolved_policy_to_proto(&self.state.store.resolve(
                workspace_id,
                SignalKind::All,
                Operation::Query,
            ))),
            store: Some(resolved_policy_to_proto(&self.state.store.resolve(
                workspace_id,
                SignalKind::All,
                Operation::Store,
            ))),
        }))
    }
}
