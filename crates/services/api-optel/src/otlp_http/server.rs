//! Connection handling for the OTLP/HTTP listener.
//!
//! `axum::serve` gives no control over hyper's connection settings, and it
//! does not install a timer, so hyper's HTTP/1 header-read timeout never
//! fires: a client can hold a connection (and a file descriptor) open forever
//! by trickling request headers ("slowloris"). [`serve`] runs the accept loop
//! itself so it can bound
//!
//! - the time before a new connection identifies its protocol (HTTP/1 request
//!   line or HTTP/2 preface); hyper's version detection has no timeout of its
//!   own, so a client that connects and sends nothing would otherwise hold the
//!   connection forever,
//! - the time to receive a complete HTTP/1 request head (hyper's
//!   `header_read_timeout`, which also closes idle keep-alive connections
//!   after the same delay), and
//! - the total time to receive a request body (`RequestBodyDeadlineLayer`);
//!   a body that is not complete in time fails with `408 Request Timeout`.
//!
//! HTTP/2 connections get keepalive pings so dead peers are detected, and a
//! cap on concurrent streams.

use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tower_http::timeout::RequestBodyDeadlineLayer;

/// Connection-level timeouts for the OTLP/HTTP listener. `None` disables one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtlpHttpServerOptions {
    /// Maximum time for a new connection to identify its protocol, and to
    /// receive a complete HTTP/1 request head, measured from when the server
    /// starts waiting for it. Also the HTTP/1 keep-alive idle limit.
    pub header_read_timeout: Option<Duration>,
    /// Maximum total time to receive a request body.
    pub body_read_timeout: Option<Duration>,
}

/// Serve `app` on `listener` until the listener fails, applying `options`.
///
/// Accepts HTTP/1.1 and HTTP/2 (prior knowledge), like `axum::serve`.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    options: OtlpHttpServerOptions,
) -> std::io::Result<()> {
    let app = match options.body_read_timeout {
        Some(limit) => app.layer(RequestBodyDeadlineLayer::new(limit)),
        None => app,
    };

    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(options.header_read_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(Some(HTTP2_KEEP_ALIVE_INTERVAL))
        .keep_alive_timeout(HTTP2_KEEP_ALIVE_TIMEOUT)
        .max_concurrent_streams(HTTP2_MAX_CONCURRENT_STREAMS);

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(error) if is_connection_error(&error) => continue,
            Err(error) => {
                // Usually EMFILE/ENFILE: back off instead of spinning.
                tracing::error!(%error, "OTLP/HTTP accept failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let service = TowerToHyperService::new(app.clone());
        let builder = builder.clone();
        let protocol_timeout = options.header_read_timeout;
        tokio::spawn(async move {
            if let Some(limit) = protocol_timeout
                && !matches!(
                    tokio::time::timeout(limit, protocol_identified(&stream)).await,
                    Ok(Ok(true))
                )
            {
                tracing::debug!(%peer, "OTLP/HTTP connection closed before sending a request");
                return;
            }
            if let Err(error) = builder
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%peer, %error, "OTLP/HTTP connection closed with error");
            }
        });
    }
}

/// HTTP/2 keepalive ping interval (dead-peer detection).
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);
/// How long to wait for a keepalive ping acknowledgement.
const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// Concurrent HTTP/2 streams per connection.
const HTTP2_MAX_CONCURRENT_STREAMS: u32 = 100;

/// Wait until the client has sent enough bytes to tell HTTP/1 from the
/// HTTP/2 connection preface. Returns `false` if the client closed first.
async fn protocol_identified(stream: &TcpStream) -> std::io::Result<bool> {
    const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    let mut buf = [0u8; H2_PREFACE.len()];
    loop {
        let n = stream.peek(&mut buf).await?;
        if n == 0 {
            return Ok(false);
        }
        if n == buf.len() || buf[..n] != H2_PREFACE[..n] {
            return Ok(true);
        }
        // A partial HTTP/2 preface: `peek` returns immediately while the same
        // bytes are buffered, so poll gently until more arrive.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Request body extractor that reports a body-read timeout as
/// `408 Request Timeout` instead of axum's generic `400 Bad Request`.
/// All other rejections (e.g. `413` over the body limit) are unchanged.
pub(crate) struct OtlpBody(pub Bytes);

impl<S: Send + Sync> FromRequest<S> for OtlpBody {
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Bytes::from_request(req, state).await {
            Ok(bytes) => Ok(Self(bytes)),
            Err(rejection) if is_body_timeout(&rejection) => {
                Err(StatusCode::REQUEST_TIMEOUT.into_response())
            }
            Err(rejection) => Err(rejection.into_response()),
        }
    }
}

fn is_body_timeout(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(err) = cause {
        if err.is::<tower_http::timeout::TimeoutError>() {
            return true;
        }
        cause = err.source();
    }
    false
}

/// Errors that only affect the connection being accepted.
fn is_connection_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use std::net::SocketAddr;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn start(options: OtlpHttpServerOptions) -> SocketAddr {
        let app = Router::new().route(
            "/v1/traces",
            post(|OtlpBody(body): OtlpBody| async move { format!("{}", body.len()) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, app, options));
        addr
    }

    /// Read until the server closes the connection; returns what was read.
    async fn read_to_close(stream: &mut TcpStream, limit: Duration) -> String {
        let mut buf = Vec::new();
        tokio::time::timeout(limit, stream.read_to_end(&mut buf))
            .await
            .expect("server closes the connection in time")
            .ok();
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn serves_complete_requests() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_secs(5)),
            body_read_timeout: Some(Duration::from_secs(5)),
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                b"POST /v1/traces HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
            )
            .await
            .unwrap();
        let response = read_to_close(&mut stream, Duration::from_secs(5)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with('3'), "{response}");
    }

    #[tokio::test]
    async fn closes_connections_with_slow_headers() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_millis(200)),
            body_read_timeout: None,
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let started = Instant::now();
        // Start a request head but never finish it.
        stream
            .write_all(b"POST /v1/traces HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        let response = read_to_close(&mut stream, Duration::from_secs(5)).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(150),
            "closed too early: {elapsed:?}"
        );
        assert!(
            !response.contains("200 OK"),
            "incomplete request must not be served: {response}"
        );
    }

    #[tokio::test]
    async fn closes_connections_that_send_nothing() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_millis(200)),
            body_read_timeout: None,
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let started = Instant::now();
        let response = read_to_close(&mut stream, Duration::from_secs(5)).await;
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert!(response.is_empty(), "{response}");
    }

    #[tokio::test]
    async fn closes_connections_stalled_mid_http2_preface() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_millis(200)),
            body_read_timeout: None,
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"PRI * HTTP/2").await.unwrap();
        let started = Instant::now();
        read_to_close(&mut stream, Duration::from_secs(5)).await;
        assert!(started.elapsed() >= Duration::from_millis(150));
    }

    #[tokio::test]
    async fn keep_alive_connection_serves_requests_within_the_timeout() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_secs(2)),
            body_read_timeout: Some(Duration::from_secs(2)),
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = b"POST /v1/traces HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nab";
        let mut buf = [0u8; 1024];
        for _ in 0..2 {
            stream.write_all(request).await.unwrap();
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
                .await
                .expect("response in time")
                .unwrap();
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            // Idle on the kept-alive connection, but less than the timeout.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[tokio::test]
    async fn serves_http2_prior_knowledge() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_secs(2)),
            body_read_timeout: Some(Duration::from_secs(2)),
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Preface + empty SETTINGS frame; the server answers with SETTINGS.
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")
            .await
            .unwrap();
        let mut frame_header = [0u8; 9];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut frame_header))
            .await
            .expect("server SETTINGS in time")
            .unwrap();
        assert_eq!(frame_header[3], 0x04, "first server frame is SETTINGS");
    }

    #[tokio::test]
    async fn times_out_slow_bodies_with_408() {
        let addr = start(OtlpHttpServerOptions {
            header_read_timeout: Some(Duration::from_secs(5)),
            body_read_timeout: Some(Duration::from_millis(200)),
        })
        .await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let started = Instant::now();
        // Promise 100 bytes, send 10, then stall.
        stream
            .write_all(
                b"POST /v1/traces HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789",
            )
            .await
            .unwrap();
        let response = read_to_close(&mut stream, Duration::from_secs(5)).await;
        assert!(
            response.starts_with(&format!(
                "HTTP/1.1 {}",
                StatusCode::REQUEST_TIMEOUT.as_u16()
            )),
            "{response}"
        );
        assert!(started.elapsed() < Duration::from_secs(4));
    }
}
