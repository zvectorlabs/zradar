//! Admission control for the OTLP ingest listeners.
//!
//! [`AdmissionLayer`] is a tower layer that bounds the number of in-flight
//! export requests across *all* connections of a listener and caps how long a
//! single request may run. When the budget is exhausted the request is rejected
//! immediately (load shedding) instead of being queued:
//!
//! | Condition   | OTLP/HTTP                        | OTLP/gRPC           |
//! |-------------|----------------------------------|---------------------|
//! | Overloaded  | `503 Service Unavailable`        | `UNAVAILABLE`       |
//! | Timed out   | `504 Gateway Timeout`            | `DEADLINE_EXCEEDED` |
//!
//! All of these are retryable per the OTLP specification, so well-behaved
//! exporters back off and retry rather than dropping data.
//!
//! Unlike tonic's built-in `concurrency_limit_per_connection`, the limit here
//! is shared across connections, so a flood spread over many connections is
//! bounded too.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::{HeaderValue, Request, Response, StatusCode, header};
use tokio::sync::Semaphore;
use tonic::Status;
use tower::{Layer, Service};

/// Wire protocol used to encode rejections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestProtocol {
    /// OTLP/HTTP: plain HTTP status codes.
    Http,
    /// OTLP/gRPC: trailers-only gRPC status responses.
    Grpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rejection {
    Overloaded,
    TimedOut,
}

/// Tower layer applying a shared concurrency limit, load shedding, and a
/// per-request timeout. See the module docs.
#[derive(Debug, Clone)]
pub struct AdmissionLayer {
    semaphore: Option<Arc<Semaphore>>,
    timeout: Option<Duration>,
    protocol: IngestProtocol,
}

impl AdmissionLayer {
    /// `max_concurrent_requests == 0` disables the concurrency limit;
    /// `timeout == None` (or zero) disables the timeout.
    pub fn new(
        max_concurrent_requests: usize,
        timeout: Option<Duration>,
        protocol: IngestProtocol,
    ) -> Self {
        let semaphore = (max_concurrent_requests > 0)
            .then(|| Arc::new(Semaphore::new(max_concurrent_requests)));
        Self {
            semaphore,
            timeout: timeout.filter(|t| !t.is_zero()),
            protocol,
        }
    }

    /// Number of request slots currently free (`None` when unlimited).
    pub fn available_permits(&self) -> Option<usize> {
        self.semaphore.as_ref().map(|s| s.available_permits())
    }
}

impl<S> Layer<S> for AdmissionLayer {
    type Service = Admission<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Admission {
            inner,
            semaphore: self.semaphore.clone(),
            timeout: self.timeout,
            protocol: self.protocol,
        }
    }
}

/// Service produced by [`AdmissionLayer`].
#[derive(Debug, Clone)]
pub struct Admission<S> {
    inner: S,
    semaphore: Option<Arc<Semaphore>>,
    timeout: Option<Duration>,
    protocol: IngestProtocol,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for Admission<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let protocol = self.protocol;

        // Shed load: never wait for a slot.
        let permit = match &self.semaphore {
            Some(semaphore) => match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let response = reject(protocol, Rejection::Overloaded);
                    return Box::pin(async move { Ok(response) });
                }
            },
            None => None,
        };

        // Call the instance that was driven to readiness; keep a fresh clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let future = inner.call(req);
        let timeout = self.timeout;

        Box::pin(async move {
            // Hold the slot until the handler has produced its response.
            let _permit = permit;
            match timeout {
                Some(limit) => match tokio::time::timeout(limit, future).await {
                    Ok(result) => result,
                    Err(_) => Ok(reject(protocol, Rejection::TimedOut)),
                },
                None => future.await,
            }
        })
    }
}

fn reject<B: Default>(protocol: IngestProtocol, rejection: Rejection) -> Response<B> {
    match protocol {
        IngestProtocol::Grpc => {
            let status = match rejection {
                Rejection::Overloaded => {
                    Status::unavailable("server overloaded: too many concurrent export requests")
                }
                Rejection::TimedOut => Status::deadline_exceeded("export request timed out"),
            };
            status.into_http()
        }
        IngestProtocol::Http => {
            let code = match rejection {
                Rejection::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
                Rejection::TimedOut => StatusCode::GATEWAY_TIMEOUT,
            };
            let mut response = Response::new(B::default());
            *response.status_mut() = code;
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::time::Instant;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    type Body = axum::body::Body;

    /// A handler that blocks until `release` is notified.
    fn gated_service(
        release: Arc<Notify>,
    ) -> impl Service<
        Request<Body>,
        Response = Response<Body>,
        Error = Infallible,
        Future = impl Future<Output = Result<Response<Body>, Infallible>> + Send,
    > + Clone
    + Send
    + 'static {
        tower::service_fn(move |_req: Request<Body>| {
            let release = release.clone();
            async move {
                release.notified().await;
                Ok::<_, Infallible>(Response::new(Body::empty()))
            }
        })
    }

    fn request() -> Request<Body> {
        Request::builder()
            .uri("/v1/traces")
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn http_sheds_second_request_when_limit_is_one() {
        let release = Arc::new(Notify::new());
        let layer = AdmissionLayer::new(1, None, IngestProtocol::Http);
        let svc = layer.layer(gated_service(release.clone()));

        let first = tokio::spawn(svc.clone().oneshot(request()));
        // Wait until the first request holds the only slot.
        while layer.available_permits() != Some(0) {
            tokio::task::yield_now().await;
        }

        let started = Instant::now();
        let second = svc.clone().oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(second.headers().get(header::RETRY_AFTER).unwrap(), "1");
        assert!(started.elapsed() < Duration::from_millis(500));

        release.notify_one();
        let first = first.await.unwrap().unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        // The slot is released once the first request completes.
        assert_eq!(layer.available_permits(), Some(1));
        let release_next = Arc::new(Notify::new());
        release_next.notify_one();
        let third = layer
            .layer(gated_service(release_next))
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(third.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn grpc_sheds_with_unavailable() {
        let release = Arc::new(Notify::new());
        let layer = AdmissionLayer::new(1, None, IngestProtocol::Grpc);
        let svc = layer.layer(gated_service(release.clone()));

        let first = tokio::spawn(svc.clone().oneshot(request()));
        while layer.available_permits() != Some(0) {
            tokio::task::yield_now().await;
        }

        let second = svc.clone().oneshot(request()).await.unwrap();
        // gRPC errors travel in a trailers-only response with HTTP 200.
        assert_eq!(second.status(), StatusCode::OK);
        let status = Status::from_header_map(second.headers()).expect("grpc-status header");
        assert_eq!(status.code(), tonic::Code::Unavailable);

        release.notify_one();
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn times_out_slow_requests() {
        let never = Arc::new(Notify::new());

        let http = AdmissionLayer::new(4, Some(Duration::from_millis(20)), IngestProtocol::Http)
            .layer(gated_service(never.clone()));
        let response = http.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        let layer = AdmissionLayer::new(4, Some(Duration::from_millis(20)), IngestProtocol::Grpc);
        let grpc = layer.layer(gated_service(never));
        let response = grpc.oneshot(request()).await.unwrap();
        let status = Status::from_header_map(response.headers()).expect("grpc-status header");
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        // A timed-out request gives its slot back.
        assert_eq!(layer.available_permits(), Some(4));
    }

    #[tokio::test]
    async fn zero_disables_limits() {
        let layer = AdmissionLayer::new(0, Some(Duration::ZERO), IngestProtocol::Http);
        assert_eq!(layer.available_permits(), None);
        // Every request waits on a barrier sized to the whole batch, so this
        // only completes if all of them are admitted concurrently.
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let svc = layer.layer(tower::service_fn(move |_req: Request<Body>| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                Ok::<_, Infallible>(Response::new(Body::empty()))
            }
        }));
        let pending: Vec<_> = (0..8)
            .map(|_| tokio::spawn(svc.clone().oneshot(request())))
            .collect();
        for handle in pending {
            let response = tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("all requests admitted")
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}
