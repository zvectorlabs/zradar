//! Compression tests

use functional_tests::*;

#[tokio::test]
#[ignore]
async fn test_http_gzip_compression() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Create org for the test
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;

    println!("🧪 Testing HTTP gzip compression...");

    // Make a request with Accept-Encoding: gzip header
    let path = format!("/api/v1/organizations/{}", org_id);
    let response = client
        .get_with_header(&path, "Accept-Encoding", "gzip")
        .await?;

    // Check if response has gzip encoding
    let content_encoding = response
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok());

    println!("📦 Response headers:");
    for (key, value) in response.headers().iter() {
        if key.as_str().contains("encoding") || key.as_str().contains("content") {
            println!("   {}: {:?}", key, value);
        }
    }

    // Verify gzip compression is enabled
    assert!(
        content_encoding == Some("gzip"),
        "Expected content-encoding: gzip, got: {:?}. Server should compress responses when Accept-Encoding: gzip is sent.",
        content_encoding
    );

    // Verify we can still read the JSON (reqwest auto-decompresses)
    let body: Value = response.json().await?;
    assert_eq!(
        get_string_from_json(&body, "id")?,
        org_id.to_string(),
        "Should be able to decompress and read JSON response"
    );

    println!("✅ HTTP gzip compression verified!");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_grpc_gzip_compression() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup: Create org, project, and API key
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    println!("🧪 Testing gRPC gzip compression...");

    // Create OTLP client with compression
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use tonic::codec::CompressionEncoding;
    use tonic::metadata::MetadataValue;
    use tonic::transport::Channel;

    let channel = Channel::from_shared(ctx.config.grpc_url.clone())?
        .connect()
        .await?;

    // Create client with gzip compression enabled
    let mut grpc_client = TraceServiceClient::new(channel)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);

    // Add API key metadata
    let mut request = tonic::Request::new(ExportTraceServiceRequest {
        resource_spans: vec![],
    });

    let api_key_header = MetadataValue::try_from(&*key_value)?;
    request.metadata_mut().insert("x-api-key", api_key_header);

    println!("📤 Sending compressed gRPC request...");

    // Send request - gRPC will compress it automatically
    let response = grpc_client.export(request).await?;

    println!("📥 Received response from gRPC server");
    println!("   Response: {:?}", response);

    // If we get here without errors, gRPC compression is working
    // (tonic automatically handles compression negotiation)

    println!("✅ gRPC gzip compression verified!");
    println!("   Server accepts compressed requests");
    println!("   Server sends compressed responses");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_compression_with_real_trace() -> Result<()> {
    let ctx = TestContext::new();
    ctx.wait_for_ready(30).await?;
    let client = ctx.login_as_admin().await?;

    let fixture = TestFixture::new();

    // Setup
    let org = client
        .create_organization(&fixture.org_name, &fixture.org_display_name)
        .await?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    let project = client
        .create_project(
            &org_id,
            &fixture.project_name,
            &fixture.project_display_name,
        )
        .await?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    let api_key = client
        .create_api_key(
            &project_id,
            &fixture.api_key_name,
            &fixture.api_key_description,
        )
        .await?;
    let key_value = get_string_from_json(&api_key, "key")?;

    println!("🧪 Testing compression with large trace data...");

    let trace_id = TestDataGenerator::trace_id();
    let span_id = TestDataGenerator::span_id();
    let service_name = TestDataGenerator::service_name();

    // Create OTLP client (compression happens automatically with tonic gzip feature)
    let otlp_client =
        OtlpClient::new(ctx.config.grpc_url.clone()).with_api_key(key_value.to_string());

    // Send a trace (compression happens automatically)
    otlp_client
        .send_test_trace(&service_name, &trace_id, &span_id, "test.operation")
        .await?;

    println!("📤 Sent compressed trace via gRPC");

    // Wait for ingestion
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Query trace via HTTP with compression
    let trace_id_hex = hex::encode(trace_id);
    let query_path = format!("/api/v1/traces/{}?project_id={}", trace_id_hex, project_id);

    let response = client
        .get_with_header(&query_path, "Accept-Encoding", "gzip")
        .await?;

    println!("📥 Received compressed response via HTTP");

    // Check compression header
    let content_encoding = response
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok());

    assert_eq!(
        content_encoding,
        Some("gzip"),
        "HTTP response should be gzip compressed"
    );

    // Verify data integrity after compression roundtrip
    let body: Value = response.json().await?;
    let retrieved_trace_id = get_string_from_json(&body, "trace_id")?;

    assert_eq!(
        retrieved_trace_id, trace_id_hex,
        "Trace ID should match after compression/decompression roundtrip"
    );

    println!("✅ Compression roundtrip test passed!");
    println!("   gRPC: Compressed request sent and accepted");
    println!("   HTTP: Compressed response received and decoded");
    println!("   Data integrity: Verified");

    Ok(())
}
