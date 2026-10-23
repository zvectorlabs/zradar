//! Axum handlers for OTLP/HTTP protocol (R1.1).

use axum::Extension;
use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use zradar_models::RequestContext;
use zradar_policy::SignalKind;
use zradar_traits::{Authenticator, SettingsRepository};

use crate::auth::{DEFAULT_AUTH_TIMEOUT, IngestAuth};
use crate::converter::OtlpConverter;
use crate::ingestion_guard::{enforce_policy_ingest, enforce_workspace_settings_and_get};
use crate::logs_converter::OtlpLogsConverter;
use crate::metrics_converter::OtlpMetricsConverter;
use crate::parser_caps::{validate_logs_request, validate_metrics_request, validate_trace_request};
use crate::retry::{retry_delay, with_retry_after, write_error_response};
use crate::score_extractor::extract_scores;
use crate::{CircuitBreaker, ProjectRateLimiter};

use super::OtlpHttpState;
use super::encoding::{ContentEncoding, decode_body};
use super::server::OtlpBody;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
/// Default request body cap for OTLP/HTTP exports.
pub const DEFAULT_MAX_BODY_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

/// Build the OTLP/HTTP axum router with the default 8 MiB body limit.
/// Mount at the server root on port 4318.
#[allow(clippy::too_many_arguments)]
pub fn otlp_http_router(
    writer: Arc<dyn zradar_traits::TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    allow_test_header_context: bool,
    settings_repo: Arc<dyn SettingsRepository>,
    rate_limiter: Arc<ProjectRateLimiter>,
    policy_enforcer: Arc<dyn zradar_policy::PolicyEnforcer>,
    circuit_breaker: Arc<CircuitBreaker>,
    cors_config: zradar_models::CorsConfig,
) -> Router {
    otlp_http_router_with_body_limit(
        writer,
        auth,
        allow_test_header_context,
        settings_repo,
        rate_limiter,
        policy_enforcer,
        circuit_breaker,
        cors_config,
        DEFAULT_MAX_BODY_BYTES,
    )
}

/// Build the OTLP/HTTP axum router with an explicit request body cap.
/// Bodies larger than `max_body_bytes` — on the wire, or once decompressed —
/// are rejected with `413 Payload Too Large`. API-key verification is limited
/// to [`DEFAULT_AUTH_TIMEOUT`]. See [`otlp_http_router_with_limits`].
#[allow(clippy::too_many_arguments)]
pub fn otlp_http_router_with_body_limit(
    writer: Arc<dyn zradar_traits::TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    allow_test_header_context: bool,
    settings_repo: Arc<dyn SettingsRepository>,
    rate_limiter: Arc<ProjectRateLimiter>,
    policy_enforcer: Arc<dyn zradar_policy::PolicyEnforcer>,
    circuit_breaker: Arc<CircuitBreaker>,
    cors_config: zradar_models::CorsConfig,
    max_body_bytes: usize,
) -> Router {
    otlp_http_router_with_limits(
        writer,
        auth,
        allow_test_header_context,
        settings_repo,
        rate_limiter,
        policy_enforcer,
        circuit_breaker,
        cors_config,
        OtlpHttpLimits {
            max_body_bytes,
            max_decoded_body_bytes: max_body_bytes,
            auth_timeout: Some(DEFAULT_AUTH_TIMEOUT),
        },
    )
}

/// Request limits for the OTLP/HTTP router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtlpHttpLimits {
    /// Maximum request body size on the wire (`413` above it).
    pub max_body_bytes: usize,
    /// Maximum request body size after `Content-Encoding` is removed (`413`
    /// above it). Also bounds uncompressed bodies.
    pub max_decoded_body_bytes: usize,
    /// Maximum time for one API-key verification; slower verifications get
    /// `503` + `Retry-After`. `None` disables the limit.
    pub auth_timeout: Option<Duration>,
}

impl Default for OtlpHttpLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_decoded_body_bytes: DEFAULT_MAX_BODY_BYTES,
            auth_timeout: Some(DEFAULT_AUTH_TIMEOUT),
        }
    }
}

/// Build the OTLP/HTTP axum router with explicit [`OtlpHttpLimits`].
///
/// Every export is authenticated from its headers, and its `Content-Type` and
/// `Content-Encoding` checked, before the body is read.
#[allow(clippy::too_many_arguments)]
pub fn otlp_http_router_with_limits(
    writer: Arc<dyn zradar_traits::TelemetryWriter>,
    auth: Option<Arc<dyn Authenticator>>,
    allow_test_header_context: bool,
    settings_repo: Arc<dyn SettingsRepository>,
    rate_limiter: Arc<ProjectRateLimiter>,
    policy_enforcer: Arc<dyn zradar_policy::PolicyEnforcer>,
    circuit_breaker: Arc<CircuitBreaker>,
    cors_config: zradar_models::CorsConfig,
    limits: OtlpHttpLimits,
) -> Router {
    let max_body_bytes = limits.max_body_bytes;
    let state = OtlpHttpState {
        writer,
        auth,
        allow_test_header_context,
        settings_repo: Some(settings_repo),
        rate_limiter: Some(rate_limiter),
        policy_enforcer: Some(policy_enforcer),
        circuit_breaker: Some(circuit_breaker),
        max_decoded_body_bytes: limits.max_decoded_body_bytes,
        auth_timeout: limits.auth_timeout,
    };

    let cors_layer = if cors_config.allowed_origins.is_empty() {
        tower_http::cors::CorsLayer::permissive()
    } else {
        let mut allowed_origins = Vec::new();
        for origin in &cors_config.allowed_origins {
            if let Ok(value) = axum::http::HeaderValue::from_str(origin) {
                allowed_origins.push(value);
            }
        }
        tower_http::cors::CorsLayer::new()
            .allow_origin(allowed_origins)
            .allow_methods([axum::http::Method::POST, axum::http::Method::OPTIONS])
            .allow_headers(tower_http::cors::Any)
    };

    let preflight_state = PreflightState {
        http: state.clone(),
        drain_limit: max_body_bytes,
    };
    Router::new()
        .route("/v1/traces", post(export_traces))
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .route_layer(axum::middleware::from_fn_with_state(
            preflight_state,
            preflight,
        ))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(cors_layer)
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

/// What an export request's headers establish, checked by [`preflight`]
/// before its body is read.
#[derive(Clone)]
struct ExportPreflight {
    context: RequestContext,
    encoding: ContentEncoding,
}

/// State for [`preflight`].
#[derive(Clone)]
struct PreflightState {
    http: OtlpHttpState,
    /// Most bytes of a rejected request's body that are drained.
    drain_limit: usize,
}

/// Route middleware that checks an export from its headers alone, before the
/// body is read: a protobuf `Content-Type` (`415`), a supported
/// `Content-Encoding` (`415`), a valid API key (`401`, or `503` +
/// `Retry-After` when the key cannot be verified right now) and the circuit
/// breaker (`503`). Rejected requests are answered at once, without buffering,
/// decompressing or parsing their body; see [`drain_rejected_body`].
async fn preflight(
    State(state): State<PreflightState>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    match check_export_headers(&state.http, request.headers()).await {
        Ok(checked) => {
            request.extensions_mut().insert(checked);
            next.run(request).await
        }
        Err(response) => {
            drain_rejected_body(request.into_body(), state.drain_limit);
            response
        }
    }
}

async fn check_export_headers(
    state: &OtlpHttpState,
    headers: &HeaderMap,
) -> Result<ExportPreflight, Response> {
    check_content_type(headers).map_err(IntoResponse::into_response)?;
    let encoding = ContentEncoding::from_headers(headers).map_err(IntoResponse::into_response)?;
    let context = IngestAuth::new(state.auth.clone(), state.allow_test_header_context)
        .with_timeout(state.auth_timeout)
        .authenticate(headers)
        .await
        .map_err(|rejection| rejection.into_http_response())?;
    if let Some(response) = check_circuit_breaker(&state.circuit_breaker).await {
        return Err(response);
    }
    Ok(ExportPreflight { context, encoding })
}

/// Rejected bodies drained at once, across all connections.
const MAX_CONCURRENT_DRAINS: usize = 64;
/// Longest time spent draining one rejected body.
const DRAIN_DEADLINE: Duration = Duration::from_secs(5);

/// Discard the unread body of a rejected HTTP/1 request in the background.
///
/// If the body were simply dropped, hyper would stop reading and close the
/// connection while the client may still be uploading; the client's TCP stack
/// can then see a reset instead of the rejection, which OTLP exporters treat
/// as a retryable network error. Draining lets the client finish its upload
/// and read the response, and keeps the connection reusable. Bytes are
/// discarded as they arrive (no buffering); a drain stops after `limit`
/// bytes or [`DRAIN_DEADLINE`], and at most [`MAX_CONCURRENT_DRAINS`] run at
/// once — beyond that the body is dropped and the connection closed.
fn drain_rejected_body(body: axum::body::Body, limit: usize) {
    use axum::body::HttpBody;

    if body.is_end_stream() {
        return;
    }
    static DRAINS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    let drains = DRAINS.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_DRAINS)));
    let Ok(permit) = drains.clone().try_acquire_owned() else {
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        let mut body = std::pin::pin!(body);
        let mut drained = 0usize;
        let _ = tokio::time::timeout(DRAIN_DEADLINE, async {
            while let Some(Ok(frame)) =
                std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await
            {
                drained = drained.saturating_add(frame.data_ref().map_or(0, |data| data.len()));
                if drained > limit {
                    break;
                }
            }
        })
        .await;
    });
}

async fn check_circuit_breaker(circuit_breaker: &Option<Arc<CircuitBreaker>>) -> Option<Response> {
    let circuit_breaker = circuit_breaker.as_ref()?;
    circuit_breaker
        .check_status()
        .await
        .err()
        .map(status_to_response)
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
    Extension(ExportPreflight { context, encoding }): Extension<ExportPreflight>,
    OtlpBody(body): OtlpBody,
) -> impl IntoResponse {
    let body = match decode_body(encoding, body, state.max_decoded_body_bytes).await {
        Ok(body) => body,
        Err(status) => return status.into_response(),
    };
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
    let settings = match enforce_workspace_settings_and_get(
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
    if !all_spans.is_empty()
        && let Err(error) = state.writer.insert_spans(&all_spans).await
    {
        return write_error_response(&error);
    }
    StatusCode::OK.into_response()
}

async fn export_metrics(
    State(state): State<OtlpHttpState>,
    Extension(ExportPreflight { context, encoding }): Extension<ExportPreflight>,
    OtlpBody(body): OtlpBody,
) -> impl IntoResponse {
    let body = match decode_body(encoding, body, state.max_decoded_body_bytes).await {
        Ok(body) => body,
        Err(status) => return status.into_response(),
    };
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
    if let Err(status) = enforce_workspace_settings_and_get(
        &state.settings_repo,
        &state.rate_limiter,
        &context,
        metrics.len() as u64,
    )
    .await
    {
        return status_to_response(status);
    }
    if !metrics.is_empty()
        && let Err(error) = state.writer.insert_metrics(&metrics).await
    {
        return write_error_response(&error);
    }
    StatusCode::OK.into_response()
}

async fn export_logs(
    State(state): State<OtlpHttpState>,
    Extension(ExportPreflight { context, encoding }): Extension<ExportPreflight>,
    OtlpBody(body): OtlpBody,
) -> impl IntoResponse {
    let body = match decode_body(encoding, body, state.max_decoded_body_bytes).await {
        Ok(body) => body,
        Err(status) => return status.into_response(),
    };
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
    if let Err(status) = enforce_workspace_settings_and_get(
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
    if !scores.is_empty()
        && let Err(error) = state.writer.insert_scores(&scores).await
    {
        return write_error_response(&error);
    }
    let logs = OtlpLogsConverter::convert(request, &context);
    if !logs.is_empty()
        && let Err(error) = state.writer.insert_logs(&logs).await
    {
        return write_error_response(&error);
    }
    StatusCode::OK.into_response()
}

/// Map a gRPC-style status to an OTLP/HTTP response. A `RetryInfo` detail
/// on the status becomes a `Retry-After` header.
fn status_to_response(status: tonic::Status) -> axum::response::Response {
    let code = match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    match retry_delay(&status) {
        Some(delay) => with_retry_after(code, delay),
        None => code.into_response(),
    }
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

    #[tokio::test]
    async fn test_cors_layer_on_otlp_http_router() {
        use axum::http::Request;
        use std::path::PathBuf;
        use tower::Service;

        struct DummyTelemetryWriter;
        #[tonic::async_trait]
        impl zradar_traits::TelemetryWriter for DummyTelemetryWriter {
            async fn insert_spans(&self, _spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_metrics(
                &self,
                _metrics: &[zradar_models::Metric],
            ) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_logs(&self, _logs: &[zradar_models::LogRecord]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_scores(
                &self,
                _scores: &[zradar_models::EvaluationScore],
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        struct DummySettingsRepo;
        #[tonic::async_trait]
        impl zradar_traits::SettingsRepository for DummySettingsRepo {
            async fn get_settings(
                &self,
                _workspace_id: zradar_models::WorkspaceId,
            ) -> anyhow::Result<Option<zradar_models::WorkspaceSettings>> {
                Ok(None)
            }
            async fn upsert_settings(
                &self,
                _settings: zradar_models::NewWorkspaceSettings,
            ) -> anyhow::Result<zradar_models::WorkspaceSettings> {
                unimplemented!()
            }
            async fn list_all_settings(
                &self,
            ) -> anyhow::Result<Vec<zradar_models::WorkspaceSettings>> {
                Ok(vec![])
            }
        }

        struct DummyPolicyEnforcer;
        #[tonic::async_trait]
        impl zradar_policy::PolicyEnforcer for DummyPolicyEnforcer {
            async fn check_ingest(
                &self,
                _ctx: zradar_policy::IngestCtx,
            ) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
            async fn check_query(&self, _ctx: zradar_policy::QueryCtx) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
        }

        let mut router = otlp_http_router(
            Arc::new(DummyTelemetryWriter),
            None,
            false,
            Arc::new(DummySettingsRepo),
            Arc::new(ProjectRateLimiter::new()),
            Arc::new(DummyPolicyEnforcer),
            Arc::new(CircuitBreaker::new(PathBuf::from("."), 100, 100, 10)),
            zradar_models::CorsConfig::default(),
        );

        // 1. Test OPTIONS preflight request
        let req = Request::builder()
            .method("OPTIONS")
            .uri("/v1/traces")
            .header("Origin", "https://example.com")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "content-type")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = router.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "*"
        );

        // 2. Test POST request CORS header presence
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("Origin", "https://example.com")
            .header("content-type", "application/x-protobuf")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = router.call(req).await.unwrap();
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "*"
        );
    }

    #[tokio::test]
    async fn test_cors_layer_with_allowlist() {
        use axum::http::Request;
        use std::path::PathBuf;
        use tower::Service;

        struct DummyTelemetryWriter;
        #[tonic::async_trait]
        impl zradar_traits::TelemetryWriter for DummyTelemetryWriter {
            async fn insert_spans(&self, _spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_metrics(
                &self,
                _metrics: &[zradar_models::Metric],
            ) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_logs(&self, _logs: &[zradar_models::LogRecord]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_scores(
                &self,
                _scores: &[zradar_models::EvaluationScore],
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        struct DummySettingsRepo;
        #[tonic::async_trait]
        impl zradar_traits::SettingsRepository for DummySettingsRepo {
            async fn get_settings(
                &self,
                _workspace_id: zradar_models::WorkspaceId,
            ) -> anyhow::Result<Option<zradar_models::WorkspaceSettings>> {
                Ok(None)
            }
            async fn upsert_settings(
                &self,
                _settings: zradar_models::NewWorkspaceSettings,
            ) -> anyhow::Result<zradar_models::WorkspaceSettings> {
                unimplemented!()
            }
            async fn list_all_settings(
                &self,
            ) -> anyhow::Result<Vec<zradar_models::WorkspaceSettings>> {
                Ok(vec![])
            }
        }

        struct DummyPolicyEnforcer;
        #[tonic::async_trait]
        impl zradar_policy::PolicyEnforcer for DummyPolicyEnforcer {
            async fn check_ingest(
                &self,
                _ctx: zradar_policy::IngestCtx,
            ) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
            async fn check_query(&self, _ctx: zradar_policy::QueryCtx) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
        }

        let mut router = otlp_http_router(
            Arc::new(DummyTelemetryWriter),
            None,
            false,
            Arc::new(DummySettingsRepo),
            Arc::new(ProjectRateLimiter::new()),
            Arc::new(DummyPolicyEnforcer),
            Arc::new(CircuitBreaker::new(PathBuf::from("."), 100, 100, 10)),
            zradar_models::CorsConfig {
                allowed_origins: vec!["https://allowed.com".to_string()],
            },
        );

        // 1. Allowed origin preflight
        let req = Request::builder()
            .method("OPTIONS")
            .uri("/v1/traces")
            .header("Origin", "https://allowed.com")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "content-type")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = router.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "https://allowed.com"
        );

        // 2. Disallowed origin preflight (should not contain access-control-allow-origin)
        let req = Request::builder()
            .method("OPTIONS")
            .uri("/v1/traces")
            .header("Origin", "https://disallowed.com")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "content-type")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = router.call(req).await.unwrap();
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
    }

    mod body_limit {
        use super::*;
        use axum::http::Request;
        use std::path::PathBuf;
        use tower::ServiceExt;

        pub(super) struct NoopWriter;
        #[tonic::async_trait]
        impl zradar_traits::TelemetryWriter for NoopWriter {
            async fn insert_spans(&self, _spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_metrics(
                &self,
                _metrics: &[zradar_models::Metric],
            ) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_logs(&self, _logs: &[zradar_models::LogRecord]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_scores(
                &self,
                _scores: &[zradar_models::EvaluationScore],
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        pub(super) struct EmptySettings;
        #[tonic::async_trait]
        impl zradar_traits::SettingsRepository for EmptySettings {
            async fn get_settings(
                &self,
                _workspace_id: zradar_models::WorkspaceId,
            ) -> anyhow::Result<Option<zradar_models::WorkspaceSettings>> {
                Ok(None)
            }
            async fn upsert_settings(
                &self,
                _settings: zradar_models::NewWorkspaceSettings,
            ) -> anyhow::Result<zradar_models::WorkspaceSettings> {
                anyhow::bail!("not supported in tests")
            }
            async fn list_all_settings(
                &self,
            ) -> anyhow::Result<Vec<zradar_models::WorkspaceSettings>> {
                Ok(vec![])
            }
        }

        pub(super) struct AllowAll;
        #[tonic::async_trait]
        impl zradar_policy::PolicyEnforcer for AllowAll {
            async fn check_ingest(
                &self,
                _ctx: zradar_policy::IngestCtx,
            ) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
            async fn check_query(&self, _ctx: zradar_policy::QueryCtx) -> zradar_policy::Decision {
                zradar_policy::Decision::Allow
            }
        }

        fn router(max_body_bytes: usize) -> Router {
            otlp_http_router_with_body_limit(
                Arc::new(NoopWriter),
                None,
                false,
                Arc::new(EmptySettings),
                Arc::new(ProjectRateLimiter::new()),
                Arc::new(AllowAll),
                Arc::new(CircuitBreaker::new(PathBuf::from("."), 100, 100, 10)),
                zradar_models::CorsConfig::default(),
                max_body_bytes,
            )
        }

        fn post(body: Vec<u8>) -> Request<axum::body::Body> {
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", PROTOBUF_CONTENT_TYPE)
                .body(axum::body::Body::from(body))
                .unwrap()
        }

        #[tokio::test]
        async fn rejects_body_over_configured_limit() {
            let response = router(16).oneshot(post(vec![0u8; 17])).await.unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        }

        #[tokio::test]
        async fn accepts_body_within_configured_limit() {
            // An empty export request is valid protobuf and fits the limit.
            let response = router(16).oneshot(post(Vec::new())).await.unwrap();
            assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            assert!(response.status().is_success(), "{}", response.status());
        }
    }

    mod retryable_rejections {
        use super::body_limit::{AllowAll, EmptySettings};
        use super::*;
        use axum::http::{Request, header};
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        use std::path::PathBuf;
        use std::time::Duration;
        use tower::ServiceExt;

        /// Refuses every write as transiently unavailable (e.g. WAL disk low).
        struct TransientWriter;
        #[tonic::async_trait]
        impl zradar_traits::TelemetryWriter for TransientWriter {
            async fn insert_spans(&self, _spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                Err(zradar_traits::WriteUnavailable::new(
                    "WAL disk space low",
                    Duration::from_secs(5),
                )
                .into())
            }
            async fn insert_metrics(
                &self,
                _metrics: &[zradar_models::Metric],
            ) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_logs(&self, _logs: &[zradar_models::LogRecord]) -> anyhow::Result<()> {
                Ok(())
            }
            async fn insert_scores(
                &self,
                _scores: &[zradar_models::EvaluationScore],
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        fn router(breaker: CircuitBreaker) -> Router {
            otlp_http_router(
                Arc::new(TransientWriter),
                None,
                false,
                Arc::new(EmptySettings),
                Arc::new(ProjectRateLimiter::new()),
                Arc::new(AllowAll),
                Arc::new(breaker),
                zradar_models::CorsConfig::default(),
            )
        }

        fn one_span_export() -> Request<axum::body::Body> {
            let request = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![2; 8],
                            name: "op".into(),
                            start_time_unix_nano: 1,
                            end_time_unix_nano: 2,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", PROTOBUF_CONTENT_TYPE)
                .body(axum::body::Body::from(request.encode_to_vec()))
                .unwrap()
        }

        #[tokio::test]
        async fn tripped_breaker_is_503_with_retry_after() {
            let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 1);
            breaker.set_queue_depth(2);
            let response = router(breaker).oneshot(one_span_export()).await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        }

        #[tokio::test]
        async fn transient_write_failure_is_503_with_retry_after() {
            let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 10);
            let response = router(breaker).oneshot(one_span_export()).await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "5");
        }

        #[test]
        fn status_mapping_keeps_retry_info() {
            let response = status_to_response(crate::retry::unavailable(
                "busy",
                Duration::from_millis(2500),
            ));
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "3");

            let response = status_to_response(tonic::Status::resource_exhausted("quota"));
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert!(response.headers().get(header::RETRY_AFTER).is_none());
        }
    }

    /// Body timeouts through the production middleware stack (body limit,
    /// CORS, admission control) and the real listener.
    mod slow_body {
        use super::body_limit::{AllowAll, EmptySettings, NoopWriter};
        use super::*;
        use crate::admission::{AdmissionLayer, IngestProtocol};
        use crate::otlp_http::{OtlpHttpServerOptions, serve};
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        use std::path::PathBuf;
        use std::time::{Duration, Instant};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        async fn start(body_read_timeout: Duration) -> std::net::SocketAddr {
            let app = otlp_http_router_with_body_limit(
                Arc::new(NoopWriter),
                None,
                false,
                Arc::new(EmptySettings),
                Arc::new(ProjectRateLimiter::new()),
                Arc::new(AllowAll),
                Arc::new(CircuitBreaker::new(PathBuf::from("."), 100, 100, 10)),
                zradar_models::CorsConfig::default(),
                DEFAULT_MAX_BODY_BYTES,
            )
            // Request timeout longer than the body timeout, as by default.
            .layer(AdmissionLayer::new(
                8,
                Some(Duration::from_secs(10)),
                IngestProtocol::Http,
            ));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(serve(
                listener,
                app,
                OtlpHttpServerOptions {
                    header_read_timeout: Some(Duration::from_secs(5)),
                    body_read_timeout: Some(body_read_timeout),
                },
            ));
            addr
        }

        fn head(content_length: usize) -> Vec<u8> {
            format!(
                "POST /v1/traces HTTP/1.1\r\nHost: x\r\nContent-Type: {PROTOBUF_CONTENT_TYPE}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
            )
            .into_bytes()
        }

        async fn response(stream: &mut TcpStream) -> String {
            let mut buf = Vec::new();
            tokio::time::timeout(Duration::from_secs(8), stream.read_to_end(&mut buf))
                .await
                .expect("response in time")
                .unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        }

        #[tokio::test]
        async fn stalled_body_gets_408_before_the_request_timeout() {
            let addr = start(Duration::from_millis(300)).await;
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let started = Instant::now();
            let mut request = head(100);
            request.extend_from_slice(&[0u8; 10]);
            stream.write_all(&request).await.unwrap();
            let response = response(&mut stream).await;
            assert!(response.starts_with("HTTP/1.1 408"), "{response}");
            assert!(started.elapsed() < Duration::from_secs(5));
        }

        #[tokio::test]
        async fn chunked_upload_within_the_deadline_succeeds() {
            let spans = (0..2_000u32)
                .map(|i| Span {
                    trace_id: vec![1; 16],
                    span_id: [i.to_be_bytes(), [2; 4]].concat(),
                    name: format!("op-{i}"),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    ..Default::default()
                })
                .collect();
            let body = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec();

            let addr = start(Duration::from_secs(3)).await;
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(&head(body.len())).await.unwrap();
            for chunk in body.chunks(body.len().div_ceil(4)) {
                stream.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let response = response(&mut stream).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
    }

    /// Checks made from headers alone, before the body is read, and request
    /// body decoding.
    mod preflight {
        use super::body_limit::{AllowAll, EmptySettings, NoopWriter};
        use super::*;
        use crate::otlp_http::encoding::tests::{gzip, zstd};
        use axum::http::{Request, header};
        use std::path::PathBuf;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use std::time::Duration;
        use tokio_stream::Stream;
        use tower::ServiceExt;
        use zradar_traits::AuthUnavailable;

        /// Accepts `Bearer good`; reports its backend down for `Bearer down`.
        struct KeyCheck;
        #[tonic::async_trait]
        impl Authenticator for KeyCheck {
            async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
                match token {
                    "good" => Ok(RequestContext::new(uuid::Uuid::nil().into())),
                    "down" => {
                        Err(AuthUnavailable::new("backend down", Duration::from_secs(2)).into())
                    }
                    _ => anyhow::bail!("Invalid API key"),
                }
            }
        }

        fn router(max_decoded_body_bytes: usize) -> Router {
            otlp_http_router_with_limits(
                Arc::new(NoopWriter),
                Some(Arc::new(KeyCheck)),
                false,
                Arc::new(EmptySettings),
                Arc::new(ProjectRateLimiter::new()),
                Arc::new(AllowAll),
                Arc::new(CircuitBreaker::new(PathBuf::from("."), 100, 100, 10)),
                zradar_models::CorsConfig::default(),
                OtlpHttpLimits {
                    max_body_bytes: 8 * 1024 * 1024,
                    max_decoded_body_bytes,
                    auth_timeout: Some(Duration::from_secs(5)),
                },
            )
        }

        /// A request body that never yields any data.
        struct NeverBody;
        impl Stream for NeverBody {
            type Item = Result<axum::body::Bytes, std::io::Error>;
            fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                Poll::Pending
            }
        }

        fn export(
            key: &str,
            encoding: Option<&str>,
            body: axum::body::Body,
        ) -> Request<axum::body::Body> {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", PROTOBUF_CONTENT_TYPE)
                .header(header::AUTHORIZATION, format!("Bearer {key}"));
            if let Some(encoding) = encoding {
                builder = builder.header(header::CONTENT_ENCODING, encoding);
            }
            builder.body(body).unwrap()
        }

        async fn send(router: Router, request: Request<axum::body::Body>) -> Response {
            tokio::time::timeout(Duration::from_secs(5), router.oneshot(request))
                .await
                .expect("answered without reading the body")
                .unwrap()
        }

        /// Rejections come from headers alone: they are answered although the
        /// body never arrives (a rejected body is only drained, in the
        /// background, never buffered or decoded).
        #[tokio::test]
        async fn rejected_requests_are_answered_without_waiting_for_the_body() {
            for (key, encoding, expected) in [
                ("bad", None, StatusCode::UNAUTHORIZED),
                ("down", None, StatusCode::SERVICE_UNAVAILABLE),
                ("good", Some("br"), StatusCode::UNSUPPORTED_MEDIA_TYPE),
            ] {
                let body = axum::body::Body::from_stream(NeverBody);
                let response = send(router(1024), export(key, encoding, body)).await;
                assert_eq!(response.status(), expected, "key {key}");
            }
        }

        #[tokio::test]
        async fn unavailable_key_backend_is_503_with_retry_after() {
            let response = send(
                router(1024),
                export("down", None, axum::body::Body::empty()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "2");
        }

        fn one_span() -> Vec<u8> {
            use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
            use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
            ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![2; 8],
                            name: "op".into(),
                            start_time_unix_nano: 1,
                            end_time_unix_nano: 2,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        }

        #[tokio::test]
        async fn compressed_exports_are_accepted() {
            let payload = one_span();
            for (encoding, body) in [("gzip", gzip(&payload)), ("zstd", zstd(&payload))] {
                let response = send(
                    router(1024),
                    export("good", Some(encoding), axum::body::Body::from(body)),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK, "{encoding}");
            }
        }

        #[tokio::test]
        async fn decompression_bombs_are_413() {
            let bomb = vec![0u8; 16 * 1024 * 1024];
            for (encoding, body) in [("gzip", gzip(&bomb)), ("zstd", zstd(&bomb))] {
                let response = send(
                    router(64 * 1024),
                    export("good", Some(encoding), axum::body::Body::from(body)),
                )
                .await;
                assert_eq!(
                    response.status(),
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "{encoding}"
                );
            }
        }

        async fn start_server() -> std::net::SocketAddr {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(crate::otlp_http::serve(
                listener,
                router(1024),
                crate::otlp_http::OtlpHttpServerOptions::default(),
            ));
            addr
        }

        fn head(key: &str, content_length: usize) -> String {
            format!(
                "POST /v1/traces HTTP/1.1\r\nHost: x\r\n\
                 Content-Type: application/x-protobuf\r\n\
                 Authorization: Bearer {key}\r\nContent-Length: {content_length}\r\n\r\n"
            )
        }

        /// Read one response (status line and headers; these have no body or a
        /// tiny one) and return its status line.
        async fn read_status(stream: &mut tokio::net::TcpStream) -> String {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            tokio::time::timeout(Duration::from_secs(5), async {
                while !buf.ends_with(b"\r\n\r\n") {
                    let n = stream.read(&mut byte).await.unwrap();
                    assert!(
                        n > 0,
                        "connection closed: {}",
                        String::from_utf8_lossy(&buf)
                    );
                    buf.push(byte[0]);
                }
            })
            .await
            .expect("response in time");
            let text = String::from_utf8_lossy(&buf).into_owned();
            // Skip a small body announced with Content-Length.
            if let Some(len) = text
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .and_then(|v| v.trim().parse::<usize>().ok())
            {
                let mut body = vec![0u8; len];
                stream.read_exact(&mut body).await.unwrap();
            }
            text.lines().next().unwrap_or_default().to_owned()
        }

        #[tokio::test]
        async fn rejected_upload_is_drained_and_the_connection_reused() {
            use tokio::io::AsyncWriteExt;
            let addr = start_server().await;
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

            // The rejection arrives before any of the body is sent.
            let body_len = 512 * 1024;
            stream
                .write_all(head("bad", body_len).as_bytes())
                .await
                .unwrap();
            assert!(read_status(&mut stream).await.starts_with("HTTP/1.1 401"));

            // The client can still upload the body, and reuse the connection.
            stream.write_all(&vec![0u8; body_len]).await.unwrap();
            stream.write_all(head("good", 0).as_bytes()).await.unwrap();
            assert!(read_status(&mut stream).await.starts_with("HTTP/1.1 200"));
        }

        #[tokio::test]
        async fn client_that_uploads_before_reading_gets_the_rejection() {
            use tokio::io::AsyncWriteExt;
            let addr = start_server().await;
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Like simple exporters: send the whole request, then read.
            let body_len = 4 * 1024 * 1024;
            let mut request = head("bad", body_len).into_bytes();
            request.extend(vec![0u8; body_len]);
            stream.write_all(&request).await.unwrap();
            assert!(read_status(&mut stream).await.starts_with("HTTP/1.1 401"));
        }

        #[tokio::test]
        async fn corrupt_compressed_body_is_400() {
            let response = send(
                router(1024),
                export("good", Some("gzip"), axum::body::Body::from(one_span())),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }
}
