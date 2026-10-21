//! End-to-end check that the OTLP gRPC admission layer sheds load on a real
//! tonic server: with a concurrency limit of 1 and a slow handler, a second
//! concurrent export is rejected immediately with `UNAVAILABLE`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use api_optel::{AdmissionLayer, IngestProtocol, TraceService, TraceServiceServer};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_client::TraceServiceClient,
};
use tokio::sync::{Notify, Semaphore};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

/// Blocks every export until `release` is notified; signals `entered` first.
struct SlowTraceService {
    entered: Arc<Semaphore>,
    release: Arc<Notify>,
}

#[tonic::async_trait]
impl TraceService for SlowTraceService {
    async fn export(
        &self,
        _request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.entered.add_permits(1);
        self.release.notified().await;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tokio::test]
async fn second_concurrent_export_is_shed_with_unavailable() {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Notify::new());
    let service = SlowTraceService {
        entered: entered.clone(),
        release: release.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = tokio::spawn(
        Server::builder()
            .layer(AdmissionLayer::new(
                1,
                Some(Duration::from_secs(30)),
                IngestProtocol::Grpc,
            ))
            .add_service(TraceServiceServer::new(service))
            .serve_with_incoming(incoming),
    );

    let endpoint = format!("http://{addr}");
    let mut first_client = TraceServiceClient::connect(endpoint.clone()).await.unwrap();
    // A separate connection: the limit must hold across connections.
    let mut second_client = TraceServiceClient::connect(endpoint).await.unwrap();

    let first = tokio::spawn(async move {
        first_client
            .export(ExportTraceServiceRequest::default())
            .await
    });
    // Wait until the first export is inside the handler, holding the slot.
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .expect("first export reached the handler")
        .unwrap()
        .forget();

    let started = Instant::now();
    let err = second_client
        .export(ExportTraceServiceRequest::default())
        .await
        .expect_err("second export must be shed");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "rejection should be immediate, took {:?}",
        started.elapsed()
    );

    release.notify_one();
    let ok = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("first export completes")
        .unwrap();
    assert!(ok.is_ok(), "{ok:?}");

    server.abort();
}
