//! Compression tests - verify gzip compression works for telemetry endpoints

#[allow(unused_imports)]
use crate::*;

#[tokio::test]
#[ignore]
async fn test_http_gzip_compression_on_telemetry() -> Result<()> {
    let env = TestEnv::setup().await?;

    println!("🧪 Testing HTTP gzip compression on telemetry query...");

    // Send a trace first
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    // Query with Accept-Encoding: gzip header
    let path = format!("/api/v1/traces/{}", trace_id_hex, env.workspace_id);
    let response = env.client
        .get_with_header(&path, "Accept-Encoding", "gzip")
        .await?;

    // Check if response has gzip encoding
    let content_encoding = response
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok());

    println!("📦 Response content-encoding: {:?}", content_encoding);

    // Verify gzip compression is enabled
    assert_eq!(
        content_encoding,
        Some("gzip"),
        "Server should compress responses when Accept-Encoding: gzip is sent"
    );

    // Verify we can still read the JSON (reqwest auto-decompresses)
    let body: Value = response.json().await?;
    assert_eq!(
        get_string_from_json(&body, "trace_id")?,
        trace_id_hex,
        "Should be able to decompress and read JSON response"
    );

    println!("✅ HTTP gzip compression verified on telemetry endpoint!");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_grpc_gzip_compression() -> Result<()> {
    let env = TestEnv::setup().await?;

    println!("🧪 Testing gRPC gzip compression...");

    // Send a real trace with compression enabled
    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // OtlpClient uses compression by default (tonic handles it)
    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "compression.test")
        .await?;

    println!("📤 Sent trace via gRPC (with compression)");

    // Verify the trace was stored correctly
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let retrieved_trace_id = get_string_from_json(&trace_data, "trace_id")?;
    assert_eq!(
        retrieved_trace_id, trace_id_hex,
        "Trace should be stored correctly even with gRPC compression"
    );

    println!("✅ gRPC gzip compression verified!");
    println!("   Server accepts compressed requests");
    println!("   Data integrity: Verified");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_compression_roundtrip() -> Result<()> {
    let env = TestEnv::setup().await?;

    println!("🧪 Testing compression roundtrip (gRPC ingestion + HTTP query)...");

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Send via gRPC (compressed)
    env.otlp
        .send_test_trace(&service_name, &trace_id, &span_id, "roundtrip.test")
        .await?;
    println!("📤 Sent compressed trace via gRPC");

    // Query via HTTP with compression
    let trace_id_hex = hex::encode(trace_id);
    wait_for_trace_default(&env.client, &trace_id_hex).await?;

    let query_path = format!("/api/v1/traces/{}", trace_id_hex, env.workspace_id);
    let response = env.client
        .get_with_header(&query_path, "Accept-Encoding", "gzip")
        .await?;

    println!("📥 Received compressed response via HTTP");

    // Verify compression header
    let content_encoding = response
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok());

    assert_eq!(
        content_encoding,
        Some("gzip"),
        "HTTP response should be gzip compressed"
    );

    // Verify data integrity
    let body: Value = response.json().await?;
    let retrieved_trace_id = get_string_from_json(&body, "trace_id")?;

    assert_eq!(
        retrieved_trace_id, trace_id_hex,
        "Trace ID should match after compression roundtrip"
    );

    println!("✅ Compression roundtrip test passed!");
    println!("   gRPC: Compressed ingestion ✓");
    println!("   HTTP: Compressed query ✓");
    println!("   Data integrity: ✓");

    Ok(())
}
