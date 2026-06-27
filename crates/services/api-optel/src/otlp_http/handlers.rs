//! Axum handlers for OTLP/HTTP protocol (R1.1).

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use prost::Message;
use std::sync::Arc;
use zradar_models::RequestContext;
use zradar_policy::SignalKind;
use zradar_traits::{Authenticator, SettingsRepository};

use crate::converter::OtlpConverter;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_project_settings_and_get};
use crate::logs_converter::OtlpLogsConverter;
use crate::metrics_converter::OtlpMetricsConverter;
use crate::parser_caps::{validate_logs_request, validate_metrics_request, validate_trace_request};
use crate::score_extractor::extract_scores;
use crate::{CircuitBreaker, ProjectRateLimiter};

use super::OtlpHttpState;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

/// Build the OTLP/HTTP axum router. Mount at the server root on port 4318.
pub fn otlp_http_router(
    writer: Arc<dyn zradar_traits::TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    allow_test_header_context: bool,
    settings_repo: Arc<dyn SettingsRepository>,
    rate_limiter: Arc<ProjectRateLimiter>,
    policy_enforcer: Arc<dyn zradar_policy::PolicyEnforcer>,
    circuit_breaker: Arc<CircuitBreaker>,
) -> Router {
    let state = OtlpHttpState {
        writer,
        auth,
        allow_test_header_context,
        settings_repo: Some(settings_repo),
        rate_limiter: Some(rate_limiter),
        policy_enforcer: Some(policy_enforcer),
        circuit_breaker: Some(circuit_breaker),
    };
    Router::new()
        .route("/v1/traces", post(export_traces))
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

/// Reject any content type that is not `application/x-protobuf`.
fn check_content_type(headers: &HeaderMap) -> Result<(), StatusCode> {
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with(PROTOBUF_CONTENT_TYPE) {
        Ok(())
    } else {
        Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
    }
}

async fn authenticate_http(
    auth: &Option<Arc<dyn Authenticator>>,
    headers: &HeaderMap,
    allow_test_header_context: bool,
) -> Result<RequestContext, StatusCode> {
    let Some(authenticator) = auth else {
        return Ok(RequestContext::default());
    };
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let mut context = authenticator
        .authenticate(token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    if allow_test_header_context {
        if let Some(tenant_id) = headers.get("x-tenant-id").and_then(|v| v.to_str().ok()) {
            context.tenant_id = tenant_id.to_string();
        }
        if let Some(project_id) = headers.get("x-project-id").and_then(|v| v.to_str().ok()) {
            context.project_id = project_id.to_string();
        }
    }

    Ok(context)
}

async fn check_circuit_breaker(circuit_breaker: &Option<Arc<CircuitBreaker>>) -> Option<Response> {
    let circuit_breaker = circuit_breaker.as_ref()?;
    if circuit_breaker.check_status().await.is_err() {
        Some(StatusCode::SERVICE_UNAVAILABLE.into_response())
    } else {
        None
    }
}

async fn enforce_http_policy(
    policy_enforcer: &Option<Arc<dyn zradar_policy::PolicyEnforcer>>,
    context: &RequestContext,
    signal: SignalKind,
    records: u64,
) -> Option<Response> {
    let policy_enforcer = policy_enforcer.as_ref()?;
    match enforce_policy_ingest(policy_enforcer.as_ref(), context, signal, records, None).await {
        Ok(()) => None,
        Err(status) => Some(status_to_response(status)),
    }
}

async fn export_traces(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(status) = check_content_type(&headers) {
        return status.into_response();
    }
    let context =
        match authenticate_http(&state.auth, &headers, state.allow_test_header_context).await {
            Ok(ctx) => ctx,
            Err(status) => return status.into_response(),
        };
    if let Some(response) = check_circuit_breaker(&state.circuit_breaker).await {
        return response;
    }
    let request =
        match opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(
            body,
        ) {
            Ok(r) => r,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
    if validate_trace_request(&request).is_err() {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let span_count = request
        .resource_spans
        .iter()
        .flat_map(|resource_spans| &resource_spans.scope_spans)
        .map(|scope_spans| scope_spans.spans.len() as u64)
        .sum();
    if let Some(response) = enforce_http_policy(
        &state.policy_enforcer,
        &context,
        SignalKind::Traces,
        span_count,
    )
    .await
    {
        return response;
    }
    let settings = match enforce_project_settings_and_get(
        &state.settings_repo,
        &state.rate_limiter,
        &context,
        span_count,
    )
    .await
    {
        Ok(settings) => settings,
        Err(status) => return status_to_response(status),
    };
    let capture_enabled = settings
        .as_ref()
        .map(|settings| settings.capture_llm_content_enabled)
        .unwrap_or(true);
    let converter = OtlpConverter::new().with_capture_enabled(capture_enabled);
    let mut all_spans = Vec::new();
    for resource_spans in request.resource_spans {
        match converter.convert_resource_spans_with(resource_spans, &context) {
            Ok(spans) => all_spans.extend(spans),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    }
    if !all_spans.is_empty() && state.writer.insert_spans(&all_spans).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

async fn export_metrics(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(status) = check_content_type(&headers) {
        return status.into_response();
    }
    let context =
        match authenticate_http(&state.auth, &headers, state.allow_test_header_context).await {
            Ok(ctx) => ctx,
            Err(status) => return status.into_response(),
        };
    if let Some(response) = check_circuit_breaker(&state.circuit_breaker).await {
        return response;
    }
    let request = match opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(body) {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if validate_metrics_request(&request).is_err() {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let metrics = OtlpMetricsConverter::convert(request, &context);
    if let Some(response) = enforce_http_policy(
        &state.policy_enforcer,
        &context,
        SignalKind::Metrics,
        metrics.len() as u64,
    )
    .await
    {
        return response;
    }
    if let Err(status) = enforce_project_settings_and_get(
        &state.settings_repo,
        &state.rate_limiter,
        &context,
        metrics.len() as u64,
    )
    .await
    {
        return status_to_response(status);
    }
    if !metrics.is_empty() && state.writer.insert_metrics(&metrics).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

async fn export_logs(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(status) = check_content_type(&headers) {
        return status.into_response();
    }
    let context =
        match authenticate_http(&state.auth, &headers, state.allow_test_header_context).await {
            Ok(ctx) => ctx,
            Err(status) => return status.into_response(),
        };
    if let Some(response) = check_circuit_breaker(&state.circuit_breaker).await {
        return response;
    }
    let request =
        match opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(
            body,
        ) {
            Ok(r) => r,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
    if validate_logs_request(&request).is_err() {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let raw_log_count = request
        .resource_logs
        .iter()
        .flat_map(|resource_logs| &resource_logs.scope_logs)
        .map(|scope_logs| scope_logs.log_records.len() as u64)
        .sum();
    if let Some(response) = enforce_http_policy(
        &state.policy_enforcer,
        &context,
        SignalKind::Logs,
        raw_log_count,
    )
    .await
    {
        return response;
    }
    if let Err(status) = enforce_project_settings_and_get(
        &state.settings_repo,
        &state.rate_limiter,
        &context,
        raw_log_count,
    )
    .await
    {
        return status_to_response(status);
    }
    // Extract evaluation scores from log attributes (R1.8): HTTP transport must
    // persist scores through the same WAL + Parquet pipeline as gRPC.
    let scores = extract_scores(&request, &context);
    if !scores.is_empty() && state.writer.insert_scores(&scores).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let logs = OtlpLogsConverter::convert(request, &context);
    if !logs.is_empty() && state.writer.insert_logs(&logs).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

fn status_to_response(status: tonic::Status) -> axum::response::Response {
    match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_check_content_type_protobuf_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-protobuf"),
        );
        assert!(check_content_type(&headers).is_ok());
    }

    #[test]
    fn test_check_content_type_json_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        assert_eq!(
            check_content_type(&headers),
            Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        );
    }

    #[test]
    fn test_check_content_type_missing_rejected() {
        let headers = HeaderMap::new();
        assert_eq!(
            check_content_type(&headers),
            Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        );
    }
}
