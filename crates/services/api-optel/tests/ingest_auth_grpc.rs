//! End-to-end checks of OTLP gRPC ingest authentication and message limits on
//! a real tonic server built like the runtime builds it: admission, then
//! [`IngestAuthLayer`], then the trace service with gzip accepted and a
//! decoded-message cap.
//!
//! - Requests are authenticated before tonic reads or decompresses the
//!   message: an oversized or compressed payload with a bad key gets
//!   `UNAUTHENTICATED`, not a size error.
//! - An unreachable key backend gets retryable `UNAVAILABLE` + `RetryInfo`.
//! - The authenticator sees all request metadata.
//! - A gzip "bomb" (small on the wire, huge decompressed) is rejected with
//!   `RESOURCE_EXHAUSTED` once decompressed output passes the cap.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api_optel::{
    AdmissionLayer, IngestAuth, IngestAuthLayer, IngestProtocol, OtlpTraceService,
    TraceServiceServer,
};
use axum::http::HeaderMap;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};
use tonic_types::StatusExt;
use zradar_models::{EvaluationScore, LogRecord, Metric, RequestContext};
use zradar_traits::{AuthUnavailable, Authenticator, TelemetryWriter};

const MAX_MESSAGE: usize = 64 * 1024;

/// `good` is valid, `down` reports the key backend unreachable, anything else
/// is rejected. Records the headers of the last call.
#[derive(Default)]
struct KeyStore {
    last_headers: Mutex<Option<HeaderMap>>,
}

#[tonic::async_trait]
impl Authenticator for KeyStore {
    async fn authenticate(&self, _token: &str) -> anyhow::Result<RequestContext> {
        unreachable!("the transports call authenticate_request")
    }

    async fn authenticate_request(
        &self,
        token: &str,
        headers: &HeaderMap,
    ) -> anyhow::Result<RequestContext> {
        *self.last_headers.lock().unwrap() = Some(headers.clone());
        match token {
            "good" => Ok(RequestContext::new(uuid::Uuid::nil().into())),
            "down" => {
                Err(AuthUnavailable::new("key store unreachable", Duration::from_secs(3)).into())
            }
            _ => anyhow::bail!("Invalid API key"),
        }
    }
}

struct NoopWriter;

#[tonic::async_trait]
impl TelemetryWriter for NoopWriter {
    async fn insert_spans(&self, _: &[zradar_models::Span]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn insert_metrics(&self, _: &[Metric]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn insert_logs(&self, _: &[LogRecord]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn insert_scores(&self, _: &[EvaluationScore]) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn start(keys: Arc<KeyStore>) -> SocketAddr {
    let auth: Option<Arc<dyn Authenticator>> = Some(keys);
    let service = OtlpTraceService::new(Arc::new(NoopWriter), auth.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(
        Server::builder()
            .layer(AdmissionLayer::new(
                16,
                Some(Duration::from_secs(30)),
                IngestProtocol::Grpc,
            ))
            .layer(IngestAuthLayer::new(IngestAuth::new(auth, false)))
            .add_service(
                TraceServiceServer::new(service)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(MAX_MESSAGE),
            )
            .serve_with_incoming(incoming),
    );
    addr
}

async fn client(addr: SocketAddr, gzip: bool) -> TraceServiceClient<Channel> {
    let client = TraceServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    if gzip {
        client.send_compressed(CompressionEncoding::Gzip)
    } else {
        client
    }
}

/// An export with one span whose name is `name_len` bytes of `a`.
fn export(key: &str, name_len: usize) -> Request<ExportTraceServiceRequest> {
    let mut request = Request::new(ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "a".repeat(name_len),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    request
}

#[tokio::test]
async fn valid_key_is_accepted_and_sees_all_metadata() {
    let keys = Arc::new(KeyStore::default());
    let addr = start(keys.clone()).await;
    let mut request = export("good", 8);
    request
        .metadata_mut()
        .insert("x-project-id", "proj-42".parse().unwrap());
    client(addr, false).await.export(request).await.unwrap();

    let seen = keys.last_headers.lock().unwrap().clone().unwrap();
    assert_eq!(seen["x-project-id"], "proj-42");
}

#[tokio::test]
async fn bad_key_is_rejected_before_the_message_is_read() {
    let addr = start(Arc::new(KeyStore::default())).await;

    // Over the message cap on the wire: decoding first would be OUT_OF_RANGE.
    let err = client(addr, false)
        .await
        .export(export("bad", MAX_MESSAGE * 2))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");

    // A decompression bomb: decompressing first would be RESOURCE_EXHAUSTED.
    let err = client(addr, true)
        .await
        .export(export("bad", MAX_MESSAGE * 64))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
}

#[tokio::test]
async fn unreachable_key_backend_is_retryable_unavailable() {
    let addr = start(Arc::new(KeyStore::default())).await;
    let err = client(addr, false)
        .await
        .export(export("down", 8))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    let retry = err
        .get_details_retry_info()
        .and_then(|info| info.retry_delay);
    assert_eq!(retry, Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn decompression_bomb_is_resource_exhausted() {
    let addr = start(Arc::new(KeyStore::default())).await;
    // ~4 MiB decoded, a few KiB on the wire: under the cap compressed, far
    // over it decompressed.
    let err = client(addr, true)
        .await
        .export(export("good", MAX_MESSAGE * 64))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err:?}");

    // A compressed export within the cap is accepted.
    client(addr, true)
        .await
        .export(export("good", MAX_MESSAGE / 2))
        .await
        .unwrap();
}
