//! gRPC handler for the `QueryService` RPC (telemetry queries).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use zradar_traits::{Capability, QueryAuthorizer};

use crate::telemetry::QueryService;

use super::auth::authorize_query;
use super::conversions::{
    log_detail_to_proto, log_filters_from_request, map_control_error, metric_filters_from_request,
    metric_series_filters_from_request, metric_series_point_to_proto, paginated_logs_to_proto,
    paginated_metrics_to_proto, paginated_spans_to_proto, paginated_traces_to_proto,
    span_detail_to_proto, span_filters_from_request, trace_detail_to_proto,
    trace_filters_from_request,
};
use super::query_proto::query_service_server::QueryService as QueryServiceRpc;
use super::query_proto::*;

/// Tonic handler that delegates to [`QueryService`].
pub struct QueryHandler {
    service: Arc<QueryService>,
    auth: Arc<dyn QueryAuthorizer>,
}

impl QueryHandler {
    pub fn new(service: Arc<QueryService>, auth: Arc<dyn QueryAuthorizer>) -> Self {
        Self { service, auth }
    }
}

#[tonic::async_trait]
impl QueryServiceRpc for QueryHandler {
    async fn query_traces(
        &self,
        request: Request<QueryTracesRequest>,
    ) -> Result<Response<QueryTracesResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadTraces).await?;
        let workspace_id = auth.workspace_id();
        let offset = req.pagination.as_ref().map(|p| p.offset).unwrap_or(0);
        let filters = trace_filters_from_request(&req);

        let page = self
            .service
            .query_traces(workspace_id, filters)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(paginated_traces_to_proto(page, offset)))
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<GetTraceResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadTraces).await?;
        let workspace_id = auth.workspace_id();

        let trace = self
            .service
            .get_trace(workspace_id, &req.trace_id)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetTraceResponse {
            trace: Some(trace_detail_to_proto(&trace)),
        }))
    }

    async fn query_spans(
        &self,
        request: Request<QuerySpansRequest>,
    ) -> Result<Response<QuerySpansResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadTraces).await?;
        let workspace_id = auth.workspace_id();
        let offset = req.pagination.as_ref().map(|p| p.offset).unwrap_or(0);
        let filters = span_filters_from_request(&req);

        let page = self
            .service
            .query_spans(workspace_id, filters)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(paginated_spans_to_proto(page, offset)))
    }

    async fn get_span(
        &self,
        request: Request<GetSpanRequest>,
    ) -> Result<Response<GetSpanResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadTraces).await?;
        let workspace_id = auth.workspace_id();

        let span = self
            .service
            .get_span(workspace_id, &req.span_id)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetSpanResponse {
            span: Some(span_detail_to_proto(&span)),
        }))
    }

    async fn query_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<QueryLogsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadLogs).await?;
        let workspace_id = auth.workspace_id();
        let offset = req.pagination.as_ref().map(|p| p.offset).unwrap_or(0);
        let filters = log_filters_from_request(&req);

        let page = self
            .service
            .query_logs(workspace_id, filters)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(paginated_logs_to_proto(page, offset)))
    }

    async fn get_log(
        &self,
        request: Request<GetLogRequest>,
    ) -> Result<Response<GetLogResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadLogs).await?;
        let workspace_id = auth.workspace_id();

        let log = self
            .service
            .get_log(workspace_id, &req.log_id)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(GetLogResponse {
            log: Some(log_detail_to_proto(&log)),
        }))
    }

    async fn query_metrics(
        &self,
        request: Request<QueryMetricsRequest>,
    ) -> Result<Response<QueryMetricsResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadMetrics).await?;
        let workspace_id = auth.workspace_id();
        let offset = req.pagination.as_ref().map(|p| p.offset).unwrap_or(0);
        let filters = metric_filters_from_request(&req);

        let page = self
            .service
            .query_metrics(workspace_id, filters)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(paginated_metrics_to_proto(page, offset)))
    }

    async fn query_metric_series(
        &self,
        request: Request<QueryMetricSeriesRequest>,
    ) -> Result<Response<QueryMetricSeriesResponse>, Status> {
        let (req, auth) = authorize_query(&self.auth, request, Capability::ReadMetrics).await?;
        let workspace_id = auth.workspace_id();
        let filters = metric_series_filters_from_request(&req);

        let points = self
            .service
            .query_metric_series(workspace_id, filters)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(QueryMetricSeriesResponse {
            points: points.iter().map(metric_series_point_to_proto).collect(),
        }))
    }
}
