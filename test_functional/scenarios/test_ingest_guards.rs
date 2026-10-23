//! Ingest authentication and request-size guards, against the running server.
//!
//! - API keys are checked from headers/metadata before the payload is read or
//!   decoded: a bad key gets `401` / `UNAUTHENTICATED` even when the body never
//!   arrives, is oversized, or is a decompression bomb.
//! - Rejected keys stay `401` / `UNAUTHENTICATED` (never a retryable status).
//! - A flood of bad keys is rejected cheaply and the server keeps serving.
//! - OTLP/HTTP accepts `gzip` and `zstd` bodies; the decoded size is capped
//!   (`413`), unknown encodings get `415`.
//! - OTLP/gRPC caps decompressed messages (`RESOURCE_EXHAUSTED`).
//! - Query and Admin gRPC pass all request metadata to the authorizer.

#[allow(unused_imports)]
use crate::*;

use std::io::Write as _;

use api::grpc::admin_proto::GetWorkspaceSettingsRequest;
use api::grpc::admin_proto::settings_service_client::SettingsServiceClient;
use api::grpc::query_proto::GetTraceRequest;
use api::grpc::query_proto::query_service_client::QueryServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codec::CompressionEncoding;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::Channel;

/// Server-side gRPC message cap (`grpc_max_decoding_message_bytes` default).
const GRPC_MAX_MESSAGE: usize = 4 * 1024 * 1024;
/// Server-side OTLP/HTTP decoded body cap (`http_max_decoded_body_bytes` default).
const HTTP_MAX_DECODED: usize = 8 * 1024 * 1024;

const BAD_KEY: &str = "zk_not_a_configured_key_0000000000";

fn otlp_http_url() -> String {
    std::env::var("TEST_OTLP_HTTP_URL").unwrap_or_else(|_| "http://localhost:4318".to_string())
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn zstd(data: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(data, 3).unwrap()
}

/// One span whose name is `name_len` bytes long.
fn trace_export(env: &TestEnv, trace_id: &[u8; 16], name_len: usize) -> ExportTraceServiceRequest {
    let mut request = env.otlp.build_test_trace(
        &TestDataGenerator::service_name(),
        trace_id,
        &TestDataGenerator::span_id(),
        "ingest.guard",
    );
    if name_len > 0 {
        request.resource_spans[0].scope_spans[0].spans[0].name = "a".repeat(name_len);
    }
    request
}

async fn post_otlp_http(
    env: &TestEnv,
    key: &str,
    encoding: Option<&str>,
    body: Vec<u8>,
) -> Result<reqwest::Response> {
    let mut request = reqwest::Client::new()
        .post(format!("{}/v1/traces", otlp_http_url()))
        .header("content-type", "application/x-protobuf")
        .header("authorization", format!("Bearer {key}"))
        .header("x-workspace-id", env.workspace_id.to_string());
    if let Some(encoding) = encoding {
        request = request.header("content-encoding", encoding);
    }
    Ok(request.body(body).send().await?)
}

async fn otlp_grpc_client(env: &TestEnv, gzip: bool) -> Result<TraceServiceClient<Channel>> {
    let client = TraceServiceClient::connect(env.grpc_url().to_string())
        .await?
        .max_encoding_message_size(usize::MAX);
    Ok(if gzip {
        client.send_compressed(CompressionEncoding::Gzip)
    } else {
        client
    })
}

fn with_key<T>(env: &TestEnv, key: &str, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {key}")).unwrap(),
    );
    request.metadata_mut().insert(
        "x-workspace-id",
        MetadataValue::try_from(env.workspace_id.to_string()).unwrap(),
    );
    request
}

// ===========================================================================
// Authentication before the payload
// ===========================================================================

/// A bad key is answered as soon as the headers arrive: the server does not
/// wait for (or read) the body. Before, it waited for the whole body and the
/// stalled upload hit the body-read timeout instead.
#[tokio::test]
#[ignore]
async fn test_otlp_http_bad_key_rejected_before_body_is_sent() -> Result<()> {
    let _env = TestEnv::setup_with_transport(Transport::Http).await?;
    let url = otlp_http_url();
    let authority = url.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = tokio::net::TcpStream::connect(authority).await?;
    // Announce a 1 MiB body and send none of it.
    let head = format!(
        "POST /v1/traces HTTP/1.1\r\nHost: {authority}\r\n\
         Content-Type: application/x-protobuf\r\nAuthorization: Bearer {BAD_KEY}\r\n\
         Content-Length: {}\r\n\r\n",
        1024 * 1024
    );
    stream.write_all(head.as_bytes()).await?;

    let mut buf = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .context("server must answer a bad key without waiting for the body")??;
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.starts_with("HTTP/1.1 401"),
        "expected 401 before the body, got: {response}"
    );
    Ok(())
}

/// A client that uploads a large body with a bad key (as exporters do: send
/// everything, then read) receives the `401`, not a connection reset that
/// exporters would retry forever.
#[tokio::test]
#[ignore]
async fn test_otlp_http_large_upload_with_bad_key_gets_401() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Http).await?;
    let trace_id = TestDataGenerator::trace_id();
    let body = trace_export(&env, &trace_id, 4 * 1024 * 1024).encode_to_vec();
    for _ in 0..3 {
        let response = post_otlp_http(&env, BAD_KEY, None, body.clone()).await?;
        assert_eq!(response.status(), 401);
    }
    Ok(())
}

/// Over gRPC a bad key is rejected by metadata alone: an oversized message or
/// a decompression bomb with a bad key gets `UNAUTHENTICATED`, not a size error
/// (which would mean the message was read and decoded first).
#[tokio::test]
#[ignore]
async fn test_otlp_grpc_bad_key_rejected_before_message_is_decoded() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Grpc).await?;
    let trace_id = TestDataGenerator::trace_id();

    for (gzip, name_len) in [
        (false, GRPC_MAX_MESSAGE + 1024),
        (true, GRPC_MAX_MESSAGE * 4),
    ] {
        let status = otlp_grpc_client(&env, gzip)
            .await?
            .export(with_key(
                &env,
                BAD_KEY,
                trace_export(&env, &trace_id, name_len),
            ))
            .await
            .expect_err("bad key must be rejected");
        assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "gzip={gzip}: {status:?}"
        );
    }
    Ok(())
}

/// Rejected keys stay non-retryable on both transports.
#[tokio::test]
#[ignore]
async fn test_invalid_ingest_key_is_not_retryable() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Grpc).await?;
    let trace_id = TestDataGenerator::trace_id();
    let body = trace_export(&env, &trace_id, 0).encode_to_vec();

    let response = post_otlp_http(&env, BAD_KEY, None, body).await?;
    assert_eq!(response.status(), 401);
    assert!(response.headers().get("retry-after").is_none());

    let status = otlp_grpc_client(&env, false)
        .await?
        .export(with_key(&env, BAD_KEY, trace_export(&env, &trace_id, 0)))
        .await
        .expect_err("bad key must be rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    Ok(())
}

/// Many concurrent bad-key exports are all rejected and the server keeps
/// accepting valid exports (bad-key warnings are rate-limited server-side).
#[tokio::test]
#[ignore]
async fn test_bad_key_flood_is_rejected_and_server_keeps_serving() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Grpc).await?;
    let client = otlp_grpc_client(&env, false).await?;
    let mut tasks = Vec::new();
    for i in 0..200 {
        let mut client = client.clone();
        let request = with_key(
            &env,
            &format!("zk_flood_{i:04}_000000000000"),
            trace_export(&env, &TestDataGenerator::trace_id(), 0),
        );
        tasks.push(tokio::spawn(async move { client.export(request).await }));
    }
    for task in tasks {
        let status = task.await?.expect_err("bad key must be rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");
    }

    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(trace_export(&env, &trace_id, 0))
        .await?;
    wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;
    Ok(())
}

// ===========================================================================
// Decoded size limits
// ===========================================================================

/// OTLP/HTTP accepts gzip- and zstd-compressed exports.
#[tokio::test]
#[ignore]
async fn test_otlp_http_accepts_gzip_and_zstd_bodies() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Http).await?;
    for encoding in ["gzip", "zstd"] {
        let trace_id = TestDataGenerator::trace_id();
        let payload = trace_export(&env, &trace_id, 0).encode_to_vec();
        let body = if encoding == "gzip" {
            gzip(&payload)
        } else {
            zstd(&payload)
        };
        let response = post_otlp_http(&env, &env.api_key, Some(encoding), body).await?;
        assert_eq!(response.status(), 200, "{encoding}");
        wait_for_trace_default(&env.client, &hex::encode(trace_id)).await?;
    }
    Ok(())
}

/// A small compressed body that expands past the decoded cap is `413`, and
/// an unsupported encoding is `415`.
#[tokio::test]
#[ignore]
async fn test_otlp_http_decompression_bomb_is_413() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Http).await?;
    let trace_id = TestDataGenerator::trace_id();
    let bomb = trace_export(&env, &trace_id, HTTP_MAX_DECODED * 2).encode_to_vec();
    for (encoding, body) in [("gzip", gzip(&bomb)), ("zstd", zstd(&bomb))] {
        assert!(
            body.len() < 1024 * 1024,
            "{encoding} bomb is small on the wire"
        );
        let response = post_otlp_http(&env, &env.api_key, Some(encoding), body).await?;
        assert_eq!(response.status(), 413, "{encoding}");
    }

    let response = post_otlp_http(&env, &env.api_key, Some("br"), b"x".to_vec()).await?;
    assert_eq!(response.status(), 415);
    Ok(())
}

/// A gzip message over the decoded cap is `RESOURCE_EXHAUSTED` over gRPC.
#[tokio::test]
#[ignore]
async fn test_otlp_grpc_decompression_bomb_is_resource_exhausted() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Grpc).await?;
    let trace_id = TestDataGenerator::trace_id();
    let status = otlp_grpc_client(&env, true)
        .await?
        .export(with_key(
            &env,
            &env.api_key,
            trace_export(&env, &trace_id, GRPC_MAX_MESSAGE * 4),
        ))
        .await
        .expect_err("decompressed message over the cap must be rejected");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
    Ok(())
}

// ===========================================================================
// Metadata forwarding on Query / Admin gRPC
// ===========================================================================

/// Metadata beyond `authorization` / `x-workspace-id` (including binary
/// entries) reaches the authorizer intact and does not break authorization.
#[tokio::test]
#[ignore]
async fn test_query_and_admin_grpc_forward_all_metadata() -> Result<()> {
    let env = TestEnv::setup_with_transport(Transport::Grpc).await?;
    let trace_id = TestDataGenerator::trace_id();
    env.otlp
        .export_traces(trace_export(&env, &trace_id, 0))
        .await?;
    let trace_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_hex).await?;

    let config = TestConfig::from_env();
    let extra = |metadata: &mut MetadataMap| {
        metadata.insert(
            "x-project-id",
            MetadataValue::from_static("proj-functional"),
        );
        metadata.insert("x-request-id", MetadataValue::from_static("req-123"));
        metadata.insert_bin(
            "x-context-bin",
            tonic::metadata::MetadataValue::from_bytes(&[0, 1, 2, 255]),
        );
    };

    let mut query = QueryServiceClient::connect(config.query_grpc_url.clone()).await?;
    let mut request = with_key(
        &env,
        &env.api_key,
        GetTraceRequest {
            workspace_id: env.workspace_id.to_string(),
            trace_id: trace_hex.clone(),
        },
    );
    extra(request.metadata_mut());
    let trace = query.get_trace(request).await?.into_inner();
    assert!(trace.trace.is_some(), "trace visible with extra metadata");

    let mut request = with_key(
        &env,
        BAD_KEY,
        GetTraceRequest {
            workspace_id: env.workspace_id.to_string(),
            trace_id: trace_hex,
        },
    );
    extra(request.metadata_mut());
    let status = query.get_trace(request).await.expect_err("bad key");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let mut admin = SettingsServiceClient::connect(config.admin_grpc_url.clone()).await?;
    let mut request = with_key(
        &env,
        &env.api_key,
        GetWorkspaceSettingsRequest {
            workspace_id: env.workspace_id.to_string(),
        },
    );
    extra(request.metadata_mut());
    admin.get_workspace_settings(request).await?;
    Ok(())
}
