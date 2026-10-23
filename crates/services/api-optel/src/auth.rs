//! API-key authentication for the OTLP ingest transports.
//!
//! Both transports authenticate from request headers alone, before the
//! request body is read or decoded, so an unauthenticated client cannot make
//! the server buffer, decompress or parse a payload:
//!
//! - OTLP/gRPC: [`IngestAuthLayer`] (a tower layer on the gRPC server) runs
//!   before tonic decodes the message and hands the resolved
//!   [`RequestContext`] to the service handlers through request extensions.
//!   Handlers used without the layer authenticate themselves
//!   ([`authenticate_grpc`]), after decoding.
//! - OTLP/HTTP: the handlers' header extractor runs before the body extractor
//!   (see `otlp_http::handlers`).
//!
//! The authenticator sees every request header (all gRPC metadata) through
//! [`Authenticator::authenticate_request`].
//!
//! | Outcome                                   | OTLP/gRPC                       | OTLP/HTTP                 |
//! |-------------------------------------------|---------------------------------|---------------------------|
//! | Missing / malformed header, rejected key  | `UNAUTHENTICATED`               | `401`                     |
//! | [`AuthUnavailable`] from the authenticator | `UNAVAILABLE` + `RetryInfo`     | `503` + `Retry-After`     |
//! | Verification exceeds the auth timeout     | `UNAVAILABLE` + `RetryInfo`     | `503` + `Retry-After`     |
//!
//! Failures are logged at `warn` at most once per [`AUTH_LOG_INTERVAL`] per
//! kind, with a count of the suppressed ones, so a flood of bad keys cannot
//! flood the logs. Keys are never logged; a short prefix identifies them.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, Request as HttpRequest, Response as HttpResponse, header};
use axum::response::{IntoResponse, Response};
use tonic::{Request, Status};
use tower::{Layer, Service};
use zradar_models::RequestContext;
use zradar_traits::{AuthUnavailable, Authenticator};

use crate::retry::{http_unavailable, unavailable};

/// Default limit on one API-key verification.
pub const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// Retry delay suggested when a verification times out.
const AUTH_TIMEOUT_RETRY_AFTER: Duration = Duration::from_secs(5);

/// Minimum interval between two warnings of the same kind.
pub const AUTH_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Client-facing message for an unavailable key backend. The cause is logged,
/// not returned: it may name internal hosts.
const UNAVAILABLE_MESSAGE: &str = "API key verification temporarily unavailable";

/// Why an ingest request was not authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthRejection {
    /// Missing or malformed credentials, or a key the authenticator rejected.
    Unauthenticated(&'static str),
    /// The key could not be verified right now; the client should retry.
    Unavailable(Duration),
}

impl AuthRejection {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::Unauthenticated(message) => Status::unauthenticated(message),
            Self::Unavailable(retry_after) => unavailable(UNAVAILABLE_MESSAGE, retry_after),
        }
    }

    pub(crate) fn into_http_response(self) -> Response {
        match self {
            Self::Unauthenticated(_) => axum::http::StatusCode::UNAUTHORIZED.into_response(),
            Self::Unavailable(retry_after) => http_unavailable(retry_after),
        }
    }
}

/// Ingest API-key verification shared by both OTLP transports.
#[derive(Clone)]
pub struct IngestAuth {
    authenticator: Option<Arc<dyn Authenticator>>,
    allow_test_header_context: bool,
    timeout: Option<Duration>,
}

impl IngestAuth {
    /// `authenticator == None` leaves ingest open. `allow_test_header_context`
    /// lets an authenticated request override its workspace with
    /// `x-workspace-id` (functional tests only). Verification is limited to
    /// [`DEFAULT_AUTH_TIMEOUT`]; see [`Self::with_timeout`].
    pub fn new(
        authenticator: Option<Arc<dyn Authenticator>>,
        allow_test_header_context: bool,
    ) -> Self {
        Self {
            authenticator,
            allow_test_header_context,
            timeout: Some(DEFAULT_AUTH_TIMEOUT),
        }
    }

    /// Limit one verification to `timeout`; `None` or zero disables the
    /// limit. A verification that runs longer is answered as "unavailable".
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout.filter(|t| !t.is_zero());
        self
    }

    fn is_enabled(&self) -> bool {
        self.authenticator.is_some()
    }

    /// Authenticate a request from its headers.
    pub(crate) async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<RequestContext, AuthRejection> {
        let Some(authenticator) = &self.authenticator else {
            return Ok(RequestContext::new(uuid::Uuid::nil().into()));
        };

        let auth_header = headers
            .get(header::AUTHORIZATION)
            .ok_or(AuthRejection::Unauthenticated(
                "Missing authorization header",
            ))?
            .to_str()
            .map_err(|_| AuthRejection::Unauthenticated("Invalid authorization header"))?;
        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or(AuthRejection::Unauthenticated("Expected: Bearer <api-key>"))?;

        let verification = authenticator.authenticate_request(token, headers);
        let result = match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, verification).await {
                Ok(result) => result,
                Err(_) => {
                    log_unavailable(&format!("verification timed out after {limit:?}"));
                    return Err(AuthRejection::Unavailable(AUTH_TIMEOUT_RETRY_AFTER));
                }
            },
            None => verification.await,
        };

        let mut context = match result {
            Ok(context) => context,
            Err(error) => {
                if let Some(transient) = AuthUnavailable::find(&error) {
                    log_unavailable(&redact(&transient.reason, token));
                    return Err(AuthRejection::Unavailable(transient.retry_after));
                }
                log_invalid_key(token, &redact(&error.to_string(), token));
                return Err(AuthRejection::Unauthenticated("Invalid API key"));
            }
        };

        if self.allow_test_header_context
            && let Some(workspace_id) = headers
                .get("x-workspace-id")
                .and_then(|value| value.to_str().ok())
                .and_then(|s| s.parse().ok())
        {
            context.workspace_id = workspace_id;
        }

        Ok(context)
    }
}

/// Request extension carrying the context resolved by [`IngestAuthLayer`].
/// Crate-private, so nothing else can insert it.
#[derive(Clone)]
struct Authenticated(RequestContext);

/// Resolve the request context in an OTLP gRPC handler.
///
/// Uses the context [`IngestAuthLayer`] resolved before the message was
/// decoded; without the layer, authenticates from the request metadata here.
pub(crate) async fn authenticate_grpc<T>(
    auth: &Option<Arc<dyn Authenticator>>,
    request: &Request<T>,
    allow_test_header_context: bool,
) -> Result<RequestContext, Status> {
    if let Some(Authenticated(context)) = request.extensions().get::<Authenticated>() {
        return Ok(context.clone());
    }
    let headers = request.metadata().clone().into_headers();
    IngestAuth::new(auth.clone(), allow_test_header_context)
        .authenticate(&headers)
        .await
        .map_err(AuthRejection::into_status)
}

/// Tower layer that authenticates OTLP gRPC requests from their metadata
/// before the message is decoded. Rejected requests never reach tonic's
/// decoder (or decompressor); accepted ones carry their [`RequestContext`] to
/// the service handlers.
///
/// Add it to the gRPC server after admission control, with the same
/// authenticator the services were built with:
///
/// ```ignore
/// Server::builder()
///     .layer(admission)
///     .layer(IngestAuthLayer::new(auth))
///     .add_service(TraceServiceServer::new(trace_service))
/// ```
#[derive(Clone)]
pub struct IngestAuthLayer {
    auth: IngestAuth,
}

impl IngestAuthLayer {
    pub fn new(auth: IngestAuth) -> Self {
        Self { auth }
    }
}

impl<S> Layer<S> for IngestAuthLayer {
    type Service = IngestAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IngestAuthService {
            inner,
            auth: self.auth.clone(),
        }
    }
}

/// Service produced by [`IngestAuthLayer`].
#[derive(Clone)]
pub struct IngestAuthService<S> {
    inner: S,
    auth: IngestAuth,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for IngestAuthService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = HttpResponse<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        // Call the instance that was driven to readiness; keep a fresh clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let auth = self.auth.clone();

        Box::pin(async move {
            // Open ingest: let the handlers resolve the (nil) context.
            if !auth.is_enabled() {
                return inner.call(req).await;
            }
            match auth.authenticate(req.headers()).await {
                Ok(context) => {
                    req.extensions_mut().insert(Authenticated(context));
                    inner.call(req).await
                }
                Err(rejection) => Ok(rejection.into_status().into_http()),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Rate-limited failure logging
// ---------------------------------------------------------------------------

/// Allows one event per interval and counts the ones it suppresses.
/// Lock-free; shared by all requests.
pub(crate) struct LogLimiter {
    interval_ms: u64,
    /// Milliseconds since [`epoch`] before which events are suppressed.
    next_allowed_ms: AtomicU64,
    suppressed: AtomicU64,
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

impl LogLimiter {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval_ms: interval.as_millis() as u64,
            next_allowed_ms: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed_since_last)` when the caller may log now.
    pub(crate) fn check(&self) -> Option<u64> {
        self.check_at(epoch().elapsed().as_millis() as u64)
    }

    fn check_at(&self, now_ms: u64) -> Option<u64> {
        let next = self.next_allowed_ms.load(Ordering::Relaxed);
        if now_ms >= next
            && self
                .next_allowed_ms
                .compare_exchange(
                    next,
                    now_ms.saturating_add(self.interval_ms),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            return Some(self.suppressed.swap(0, Ordering::Relaxed));
        }
        self.suppressed.fetch_add(1, Ordering::Relaxed);
        None
    }
}

static INVALID_KEY_LOG: LogLimiter = LogLimiter::new(AUTH_LOG_INTERVAL);
static UNAVAILABLE_LOG: LogLimiter = LogLimiter::new(AUTH_LOG_INTERVAL);

/// Longest authenticator message that is logged.
const MAX_LOGGED_ERROR_CHARS: usize = 200;

fn log_invalid_key(token: &str, error: &str) {
    if let Some(suppressed) = INVALID_KEY_LOG.check() {
        tracing::warn!(
            key_prefix = %key_prefix(token),
            error = %error,
            suppressed,
            "OTLP ingest API key rejected (further rejections are logged at most every {}s)",
            AUTH_LOG_INTERVAL.as_secs()
        );
    }
}

fn log_unavailable(reason: &str) {
    if let Some(suppressed) = UNAVAILABLE_LOG.check() {
        tracing::warn!(
            reason = %reason,
            suppressed,
            "OTLP ingest API key verification unavailable; answering retryable error"
        );
    }
}

/// A short, non-secret hint identifying a key: its first four characters when
/// the key is long enough for that to reveal little, else nothing.
pub(crate) fn key_prefix(token: &str) -> String {
    const SHOWN: usize = 4;
    const MIN_LEN: usize = 16;
    match token.get(..SHOWN) {
        Some(prefix) if token.len() >= MIN_LEN => format!("{prefix}… ({} chars)", token.len()),
        _ => format!("<redacted> ({} chars)", token.len()),
    }
}

/// Remove `token` from an authenticator message and bound its length.
pub(crate) fn redact(message: &str, token: &str) -> String {
    let message = if token.is_empty() {
        message.to_owned()
    } else {
        message.replace(token, "<redacted>")
    };
    match message.char_indices().nth(MAX_LOGGED_ERROR_CHARS) {
        Some((cut, _)) => format!("{}…", &message[..cut]),
        None => message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::retry_delay;
    use std::convert::Infallible;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use tower::ServiceExt;

    /// Accepts `valid`, reports the backend down for `down`, hangs for
    /// `hang`, and rejects everything else. Records the headers it saw.
    #[derive(Default)]
    struct MockAuthenticator {
        seen_headers: Mutex<Option<HeaderMap>>,
        calls: AtomicUsize,
    }

    #[tonic::async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(&self, _token: &str) -> anyhow::Result<RequestContext> {
            unreachable!("transports call authenticate_request")
        }

        async fn authenticate_request(
            &self,
            token: &str,
            headers: &HeaderMap,
        ) -> anyhow::Result<RequestContext> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.seen_headers.lock().unwrap() = Some(headers.clone());
            match token {
                "valid" => Ok(RequestContext::new(uuid::Uuid::nil().into())),
                "down" => Err(anyhow::Error::new(AuthUnavailable::new(
                    "key store unreachable",
                    Duration::from_secs(7),
                ))
                .context("verifying key")),
                "hang" => std::future::pending().await,
                _ => anyhow::bail!("invalid token {token}"),
            }
        }
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, value.parse().unwrap());
        }
        map
    }

    fn ingest_auth(mock: &Arc<MockAuthenticator>, test_headers: bool) -> IngestAuth {
        IngestAuth::new(Some(mock.clone() as Arc<dyn Authenticator>), test_headers)
    }

    #[tokio::test]
    async fn valid_key_resolves_context() {
        let mock = Arc::new(MockAuthenticator::default());
        let ctx = ingest_auth(&mock, false)
            .authenticate(&headers(&[("authorization", "Bearer valid")]))
            .await
            .unwrap();
        assert_eq!(ctx.workspace_id, uuid::Uuid::nil().into());
    }

    #[tokio::test]
    async fn missing_malformed_and_invalid_keys_are_unauthenticated() {
        let mock = Arc::new(MockAuthenticator::default());
        let auth = ingest_auth(&mock, false);
        for (case, expected) in [
            (headers(&[]), "Missing authorization header"),
            (
                headers(&[("authorization", "Basic abc")]),
                "Expected: Bearer <api-key>",
            ),
            (
                headers(&[("authorization", "Bearer wrong")]),
                "Invalid API key",
            ),
        ] {
            let rejection = auth.authenticate(&case).await.unwrap_err();
            assert_eq!(rejection, AuthRejection::Unauthenticated(expected));
            assert_eq!(
                rejection.clone().into_status().code(),
                tonic::Code::Unauthenticated
            );
            assert_eq!(
                rejection.into_http_response().status(),
                axum::http::StatusCode::UNAUTHORIZED
            );
        }
    }

    #[tokio::test]
    async fn unavailable_backend_is_retryable() {
        let mock = Arc::new(MockAuthenticator::default());
        let rejection = ingest_auth(&mock, false)
            .authenticate(&headers(&[("authorization", "Bearer down")]))
            .await
            .unwrap_err();
        assert_eq!(
            rejection,
            AuthRejection::Unavailable(Duration::from_secs(7))
        );

        let status = rejection.clone().into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(retry_delay(&status), Some(Duration::from_secs(7)));
        // The backend's own message is not sent to the client.
        assert!(!status.message().contains("key store"));

        let response = rejection.into_http_response();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()[header::RETRY_AFTER], "7");
    }

    #[tokio::test]
    async fn slow_backend_times_out_as_unavailable() {
        let mock = Arc::new(MockAuthenticator::default());
        let rejection = ingest_auth(&mock, false)
            .with_timeout(Some(Duration::from_millis(50)))
            .authenticate(&headers(&[("authorization", "Bearer hang")]))
            .await
            .unwrap_err();
        assert_eq!(
            rejection,
            AuthRejection::Unavailable(AUTH_TIMEOUT_RETRY_AFTER)
        );
    }

    #[tokio::test]
    async fn authenticator_sees_all_headers() {
        let mock = Arc::new(MockAuthenticator::default());
        ingest_auth(&mock, false)
            .authenticate(&headers(&[
                ("authorization", "Bearer valid"),
                ("x-project-id", "p-1"),
                ("x-custom-tenant", "t-9"),
            ]))
            .await
            .unwrap();
        let seen = mock.seen_headers.lock().unwrap().clone().unwrap();
        assert_eq!(seen["x-project-id"], "p-1");
        assert_eq!(seen["x-custom-tenant"], "t-9");
    }

    #[tokio::test]
    async fn header_context_applies_only_in_test_mode() {
        let mock = Arc::new(MockAuthenticator::default());
        let request = headers(&[
            ("authorization", "Bearer valid"),
            ("x-workspace-id", "00000000-0000-0000-0000-000000000001"),
        ]);
        let ctx = ingest_auth(&mock, false)
            .authenticate(&request)
            .await
            .unwrap();
        assert_eq!(ctx.workspace_id, uuid::Uuid::nil().into());

        let ctx = ingest_auth(&mock, true)
            .authenticate(&request)
            .await
            .unwrap();
        assert_eq!(
            ctx.workspace_id.to_string(),
            "00000000-0000-0000-0000-000000000001"
        );
    }

    #[tokio::test]
    async fn open_ingest_needs_no_key() {
        let ctx = IngestAuth::new(None, false)
            .authenticate(&HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(ctx.workspace_id, uuid::Uuid::nil().into());
    }

    #[tokio::test]
    async fn grpc_handler_authenticates_without_the_layer() {
        let mock = Arc::new(MockAuthenticator::default());
        let auth: Option<Arc<dyn Authenticator>> = Some(mock.clone());
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer wrong".parse().unwrap());
        let status = authenticate_grpc(&auth, &request, false).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);

        request
            .metadata_mut()
            .insert("authorization", "Bearer valid".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-project-id", "p-2".parse().unwrap());
        authenticate_grpc(&auth, &request, false).await.unwrap();
        // All metadata reaches the authenticator.
        let seen = mock.seen_headers.lock().unwrap().clone().unwrap();
        assert_eq!(seen["x-project-id"], "p-2");
    }

    /// A gRPC "handler" that records whether it ran and the context it got.
    fn grpc_inner(
        reached: Arc<AtomicUsize>,
    ) -> impl Service<
        HttpRequest<axum::body::Body>,
        Response = HttpResponse<axum::body::Body>,
        Error = Infallible,
        Future = impl Future<Output = Result<HttpResponse<axum::body::Body>, Infallible>> + Send,
    > + Clone
    + Send
    + 'static {
        tower::service_fn(move |req: HttpRequest<axum::body::Body>| {
            let reached = reached.clone();
            async move {
                reached.fetch_add(1, Ordering::SeqCst);
                assert!(req.extensions().get::<Authenticated>().is_some());
                Ok::<_, Infallible>(HttpResponse::new(axum::body::Body::empty()))
            }
        })
    }

    fn grpc_request(key: &str) -> HttpRequest<axum::body::Body> {
        HttpRequest::builder()
            .uri("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
            .header("authorization", format!("Bearer {key}"))
            .header("content-type", "application/grpc")
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn layer_rejects_before_the_handler_runs() {
        let mock = Arc::new(MockAuthenticator::default());
        let reached = Arc::new(AtomicUsize::new(0));
        let svc =
            IngestAuthLayer::new(ingest_auth(&mock, false)).layer(grpc_inner(reached.clone()));

        let response = svc.clone().oneshot(grpc_request("wrong")).await.unwrap();
        let status = Status::from_header_map(response.headers()).expect("grpc-status");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);

        let response = svc.clone().oneshot(grpc_request("down")).await.unwrap();
        let status = Status::from_header_map(response.headers()).expect("grpc-status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(retry_delay(&status), Some(Duration::from_secs(7)));
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let response = svc.oneshot(grpc_request("valid")).await.unwrap();
        assert!(Status::from_header_map(response.headers()).is_none());
        assert_eq!(reached.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handler_uses_the_context_resolved_by_the_layer() {
        let mock = Arc::new(MockAuthenticator::default());
        let auth: Option<Arc<dyn Authenticator>> = Some(mock.clone());
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(Authenticated(RequestContext::new(uuid::Uuid::max().into())));
        let ctx = authenticate_grpc(&auth, &request, false).await.unwrap();
        assert_eq!(ctx.workspace_id, uuid::Uuid::max().into());
        // Verified once, by the layer; not again in the handler.
        assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn log_limiter_allows_one_event_per_interval_and_counts_the_rest() {
        let limiter = LogLimiter::new(Duration::from_secs(10));
        assert_eq!(limiter.check_at(0), Some(0));
        for t in 1..=500 {
            assert_eq!(limiter.check_at(t), None);
        }
        assert_eq!(limiter.check_at(9_999), None);
        assert_eq!(limiter.check_at(10_000), Some(501));
        assert_eq!(limiter.check_at(10_001), None);
        assert_eq!(limiter.check_at(25_000), Some(1));
    }

    #[test]
    fn keys_are_never_logged_whole() {
        let key = "zk_live_0123456789abcdef";
        let hint = key_prefix(key);
        assert_eq!(hint, "zk_l… (24 chars)");
        assert!(!hint.contains(key));
        assert_eq!(key_prefix("short"), "<redacted> (5 chars)");

        let message = redact(&format!("unknown key {key} for tenant"), key);
        assert!(!message.contains(key));
        assert_eq!(message, "unknown key <redacted> for tenant");

        let long = "x".repeat(1_000);
        assert!(redact(&long, key).chars().count() <= MAX_LOGGED_ERROR_CHARS + 1);
    }
}
