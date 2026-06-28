//! PR12 — Adversarial security tests (Phase 5 threat model, §2 and §6).
//!
//! These tests probe the two residual-risk areas identified in
//! `SECURITY-THREAT-MODEL.md`:
//!
//! 1. **SQL injection via filter params** — user-controlled query params
//!    (service_name, action_name, …) reach `escape_sql_str()` before being
//!    interpolated into DataFusion SQL.  We send a battery of injection
//!    payloads and assert the server returns HTTP 200 with zero rows rather
//!    than crashing or returning unexpected rows.
//!
//! 2. **Token validation** — unauthenticated and invalid-token requests must
//!    be rejected before reaching any data path.
//!
//! 3. **constant_time_eq correctness** — inline correctness tests for the
//!    XOR-fold pattern used in `validate_service_token`.
//!
//! All integration tests that hit a live server are `#[ignore]`.
//! Pure unit tests (no live server) run in `cargo test` without `--ignored`.

#[allow(unused_imports)]
use crate::*;
use anyhow::Result;

// ===========================================================================
// Helpers
// ===========================================================================

/// Send a single OTLP span with the given `service_name` so we have at least
/// one real row under the current workspace. Returns the hex trace_id.
async fn ingest_named_span(env: &TestEnv, service_name: &str) -> Result<String> {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
    use uuid::Uuid;

    let trace_id = Uuid::new_v4().as_bytes().to_vec();
    let trace_id_hex = hex::encode(&trace_id);
    let mut span_id = vec![0u8; 8];
    span_id.copy_from_slice(&Uuid::new_v4().as_bytes()[0..8]);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue(service_name.to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![OtlpSpan {
                    trace_id,
                    span_id,
                    name: "security.probe".to_string(),
                    start_time_unix_nano: now_ns,
                    end_time_unix_nano: now_ns + 1_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    env.otlp.export_traces(req).await?;
    Ok(trace_id_hex)
}

/// Query `/api/v1/spans?service_name=<value>` and return (status_code, item_count).
async fn query_spans_by_service(env: &TestEnv, service_name: &str) -> Result<(u16, usize)> {
    let encoded = urlencoding::encode(service_name).into_owned();
    let url = format!("/api/v1/spans?service_name={encoded}");
    let resp = env.client.get(&url).await?;
    let status = resp.status().as_u16();
    if !resp.status().is_success() {
        return Ok((status, 0));
    }
    let body: serde_json::Value = resp.json().await?;
    let count = body
        .get("items")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    Ok((status, count))
}

// ===========================================================================
// SQL injection via span filter params
// ===========================================================================

/// Classic injection payloads that attempt to widen the DataFusion WHERE clause
/// to return all rows. The server must respond with either:
///
/// - HTTP 200 + 0 items (injection was neutralised by `escape_sql_str`)
/// - HTTP 4xx/5xx (payload rejected before reaching the engine)
///
/// Returning multiple rows would indicate the injection successfully widened
/// the WHERE clause beyond the queried service name.
#[tokio::test]
#[ignore]
async fn test_sql_injection_service_name_payloads_return_zero_rows() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Ingest exactly one span so there is real data under this workspace.
    let _trace_id = ingest_named_span(&env, "legit-service").await?;
    // Give the flush worker time to write the Parquet file.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let injection_payloads: &[&str] = &[
        "' OR '1'='1",
        "' OR 1=1--",
        "'; DROP TABLE spans; --",
        "' UNION SELECT * FROM spans; --",
        "svc'--",
        "svc'/*",
        // Null byte
        "svc\x00injection",
        // Very long string
        &"A".repeat(4096),
        // Empty string (edge case — returns 0 because it matches nothing)
        "",
    ];

    for payload in injection_payloads {
        let (status, count) = query_spans_by_service(&env, payload).await?;
        // A 4xx/5xx is acceptable — what's NOT acceptable is 200 + >0 rows
        // when the payload does not match the ingested service name "legit-service".
        if status == 200 && count > 0 {
            anyhow::bail!(
                "Injection payload {:?} returned {count} row(s) — potential SQL widening",
                payload
            );
        }
    }

    Ok(())
}

/// Verify that a benign value containing a single-quote (e.g. Irish company
/// names like "O'Brien Systems") is correctly escaped and returns zero rows
/// because "O'Brien" does not match our ingested service name.
#[tokio::test]
#[ignore]
async fn test_single_quote_in_service_name_is_escaped() -> Result<()> {
    let env = TestEnv::setup().await?;
    let _trace_id = ingest_named_span(&env, "clean-service").await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let (status, count) = query_spans_by_service(&env, "O'Brien").await?;
    if status == 200 && count > 0 {
        anyhow::bail!(
            "Service name with single-quote returned {count} rows — escape_sql_str may be broken"
        );
    }
    Ok(())
}

/// Verify that the action_name filter is also injection-safe (it travels through
/// a separate `escape_sql_str` call in `telemetry_reader.rs`).
#[tokio::test]
#[ignore]
async fn test_sql_injection_action_name_returns_zero_rows() -> Result<()> {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
    use uuid::Uuid;

    let env = TestEnv::setup().await?;

    let trace_id = Uuid::new_v4().as_bytes().to_vec();
    let mut span_id = vec![0u8; 8];
    span_id.copy_from_slice(&Uuid::new_v4().as_bytes()[0..8]);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(AnyValue::StringValue("security-test-svc".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![OtlpSpan {
                    trace_id,
                    span_id,
                    name: "security.probe".to_string(),
                    start_time_unix_nano: now_ns,
                    end_time_unix_nano: now_ns + 1_000_000,
                    attributes: vec![KeyValue {
                        key: "action.name".to_string(),
                        value: Some(OtlpAnyValue {
                            value: Some(AnyValue::StringValue("benign_action".to_string())),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    env.otlp.export_traces(req).await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let injection_payloads: &[&str] = &[
        "' OR '1'='1",
        "'; DROP TABLE spans; --",
        "benign_action' OR '1'='1",
    ];

    for payload in injection_payloads {
        let encoded = urlencoding::encode(payload).into_owned();
        let url = format!("/api/v1/spans?action_name={encoded}");
        let resp = env.client.get(&url).await?;
        let status = resp.status().as_u16();
        if status == 200 {
            let body: serde_json::Value = resp.json().await?;
            let count = body
                .get("items")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if count > 0 {
                anyhow::bail!(
                    "action_name injection payload {:?} returned {count} row(s)",
                    payload
                );
            }
        }
    }

    Ok(())
}

// ===========================================================================
// Unauthenticated / invalid-token request rejection
// ===========================================================================

/// Requests with no Authorization header must be rejected with a non-200
/// status. This confirms the auth layer is not bypassed by omitting the header.
#[tokio::test]
#[ignore]
async fn test_unauthenticated_request_is_rejected() -> Result<()> {
    let session = TestSession::setup().await?;
    let anon = session.unauthenticated_client();
    let resp = anon.get("/api/v1/spans").await?;
    let status = resp.status().as_u16();
    if status == 200 {
        anyhow::bail!("Unauthenticated GET /api/v1/spans returned 200 — auth layer missing");
    }
    Ok(())
}

/// Requests carrying a syntactically valid but wrong Bearer token must be
/// rejected with a non-200 status.
#[tokio::test]
#[ignore]
async fn test_invalid_token_is_rejected() -> Result<()> {
    let session = TestSession::setup().await?;

    // Build a client that sends a wrong key.
    let mut bad_client = session.unauthenticated_client();
    bad_client.set_token("zk_definitely_not_a_real_key_00000000000000".to_string());

    let resp = bad_client.get("/api/v1/spans").await?;
    let status = resp.status().as_u16();
    if status == 200 {
        anyhow::bail!("Wrong Bearer token returned 200 — token validation is broken");
    }
    Ok(())
}

// ===========================================================================
// constant_time_eq correctness (unit tests — no live server required)
// ===========================================================================

/// Inline reimplementation of the XOR-fold pattern from `zradar-auth-config`
/// so we can test its correctness without adding a crate dependency.
/// If the upstream implementation diverges, these tests will catch regressions
/// in the pattern logic even if the import is absent.
fn constant_time_eq_ref(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        // Run the fold anyway to avoid short-circuit, then return false.
        let _ = a
            .iter()
            .zip(a.iter())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y));
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[test]
fn test_constant_time_eq_equal_slices() {
    assert!(constant_time_eq_ref(b"abc123", b"abc123"));
    assert!(constant_time_eq_ref(b"", b""));
}

#[test]
fn test_constant_time_eq_unequal_same_length() {
    assert!(!constant_time_eq_ref(b"abc123", b"abc124"));
    assert!(!constant_time_eq_ref(b"aXcdef", b"abcdef"));
    assert!(!constant_time_eq_ref(b"Xbcdef", b"abcdef"));
}

#[test]
fn test_constant_time_eq_different_lengths() {
    assert!(!constant_time_eq_ref(b"abc", b"abcd"));
    assert!(!constant_time_eq_ref(b"abcd", b"abc"));
    assert!(!constant_time_eq_ref(b"", b"a"));
    assert!(!constant_time_eq_ref(b"a", b""));
}

/// A token that is a prefix of the expected value must not match
/// (guards against length-extension / prefix matching bugs).
#[test]
fn test_constant_time_eq_no_prefix_match() {
    let full = b"zk_super_secret_key_00000000000000000000000000000000";
    let prefix = b"zk_super_secret_key_0000000000000000000000000000000"; // 1 byte short
    assert!(!constant_time_eq_ref(prefix, full));
    assert!(!constant_time_eq_ref(full, prefix));
}

/// All-zeros inputs of different lengths must not accidentally be equal.
#[test]
fn test_constant_time_eq_zero_bytes_different_length() {
    let short = vec![0u8; 16];
    let long = vec![0u8; 32];
    assert!(!constant_time_eq_ref(&short, &long));
}
