//! gRPC handler for the `AnalyticsService` RPC (dashboard analytics).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_traits::{Capability, QueryAuthorizer};

use crate::telemetry::QueryService;

use super::auth::authorize_query;
use super::conversions::{
    agent_analytics_to_proto, analytics_query_from_agent, analytics_query_from_get_analytics,
    analytics_query_from_llm, analytics_query_from_metrics_summary, analytics_result_to_proto,
    error_analytics_from_request, error_breakdown_to_proto, guardrails_analytics_query,
    guardrails_analytics_to_proto, ingest_rate_query_from_request, ingest_rate_to_proto,
    llm_analytics_to_proto, map_control_error, metrics_summary_to_proto,
    query_usage_query_from_request, query_usage_to_proto, quota_status_query_from_request,
    quota_status_to_proto, storage_usage_daily_query_from_request, storage_usage_daily_to_proto,
    storage_usage_query_from_request, storage_usage_to_proto, top_endpoint_to_proto,
    top_n_query_from_request, usage_daily_query_from_request, usage_daily_to_proto,
};
use super::query_proto::analytics_service_server::AnalyticsService as AnalyticsServiceRpc;
use super::query_proto::*;

/// Tonic handler that delegates to [`QueryService`].
pub struct AnalyticsHandler {
    service: Arc<QueryService>,
    auth: Arc<dyn QueryAuthorizer>,
}

impl AnalyticsHandler {
    pub fn new(service: Arc<QueryService>, auth: Arc<dyn QueryAuthorizer>) -> Self {
        Self { service, auth }
    }
}

#[tonic::async_trait]
impl AnalyticsServiceRpc for AnalyticsHandler {
    async fn get_analytics(
        &self,
        request: Request<GetAnalyticsRequest>,
    ) -> Result<Response<GetAnalyticsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = analytics_query_from_get_analytics(&req);

        let results = self
            .service
            .get_analytics(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetAnalyticsResponse {
            results: results.iter().map(analytics_result_to_proto).collect(),
        }))
    }

    async fn get_metrics_summary(
        &self,
        request: Request<GetMetricsSummaryRequest>,
    ) -> Result<Response<GetMetricsSummaryResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadMetrics).await?;
        let workspace_id = auth.workspace_id();
        let query = analytics_query_from_metrics_summary(&req);

        let summary = self
            .service
            .get_metrics_summary(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(metrics_summary_to_proto(&summary)))
    }

    async fn get_top_endpoints(
        &self,
        request: Request<GetTopEndpointsRequest>,
    ) -> Result<Response<GetTopEndpointsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = top_n_query_from_request(&req)?;

        let endpoints = self
            .service
            .get_top_endpoints(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetTopEndpointsResponse {
            endpoints: endpoints.iter().map(top_endpoint_to_proto).collect(),
        }))
    }

    async fn get_error_breakdown(
        &self,
        request: Request<GetErrorBreakdownRequest>,
    ) -> Result<Response<GetErrorBreakdownResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = error_analytics_from_request(&req)?;

        let errors = self
            .service
            .get_error_breakdown(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetErrorBreakdownResponse {
            errors: errors.iter().map(error_breakdown_to_proto).collect(),
        }))
    }

    async fn get_llm_analytics(
        &self,
        request: Request<GetLlmAnalyticsRequest>,
    ) -> Result<Response<GetLlmAnalyticsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = analytics_query_from_llm(&req);

        let items = self
            .service
            .get_llm_analytics(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetLlmAnalyticsResponse {
            items: items.iter().map(llm_analytics_to_proto).collect(),
        }))
    }

    async fn get_agent_analytics(
        &self,
        request: Request<GetAgentAnalyticsRequest>,
    ) -> Result<Response<GetAgentAnalyticsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = analytics_query_from_agent(&req);

        let items = self
            .service
            .get_agent_analytics(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetAgentAnalyticsResponse {
            items: items.iter().map(agent_analytics_to_proto).collect(),
        }))
    }

    async fn get_guardrails_analytics(
        &self,
        request: Request<GetGuardrailsAnalyticsRequest>,
    ) -> Result<Response<GetGuardrailsAnalyticsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = guardrails_analytics_query(&req);

        let analytics = self
            .service
            .get_guardrails_analytics(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(guardrails_analytics_to_proto(&analytics)))
    }

    async fn get_storage_usage(
        &self,
        request: Request<GetStorageUsageRequest>,
    ) -> Result<Response<GetStorageUsageResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = storage_usage_query_from_request(&req);

        let items = self
            .service
            .get_storage_usage(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetStorageUsageResponse {
            items: items.iter().map(storage_usage_to_proto).collect(),
        }))
    }

    async fn get_storage_usage_daily(
        &self,
        request: Request<GetStorageUsageDailyRequest>,
    ) -> Result<Response<GetStorageUsageDailyResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = storage_usage_daily_query_from_request(&req);

        let items = self
            .service
            .get_storage_usage_daily(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetStorageUsageDailyResponse {
            items: items.iter().map(storage_usage_daily_to_proto).collect(),
        }))
    }

    async fn get_quota_status(
        &self,
        request: Request<GetQuotaStatusRequest>,
    ) -> Result<Response<GetQuotaStatusResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = quota_status_query_from_request(&req);

        let items = self
            .service
            .get_quota_status(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetQuotaStatusResponse {
            items: items.iter().map(quota_status_to_proto).collect(),
        }))
    }

    async fn get_usage_daily(
        &self,
        request: Request<GetUsageDailyRequest>,
    ) -> Result<Response<GetUsageDailyResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = usage_daily_query_from_request(&req);

        let items = self
            .service
            .get_usage_daily(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetUsageDailyResponse {
            items: items.iter().map(usage_daily_to_proto).collect(),
        }))
    }

    async fn get_ingest_rate(
        &self,
        request: Request<GetIngestRateRequest>,
    ) -> Result<Response<GetIngestRateResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = ingest_rate_query_from_request(&req);

        let items = self
            .service
            .get_ingest_rate(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetIngestRateResponse {
            items: items.iter().map(ingest_rate_to_proto).collect(),
        }))
    }

    async fn get_query_usage(
        &self,
        request: Request<GetQueryUsageRequest>,
    ) -> Result<Response<GetQueryUsageResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadDashboards).await?;
        let workspace_id = auth.workspace_id();
        let query = query_usage_query_from_request(&req);

        let items = self
            .service
            .get_query_usage(workspace_id, query)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetQueryUsageResponse {
            items: items.iter().map(query_usage_to_proto).collect(),
        }))
    }
}
