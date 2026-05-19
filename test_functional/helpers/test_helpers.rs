//! Common test helper functions

use anyhow::{Context, Result};
use reqwest::Client;
use std::time::Duration;

/// Wait for server to be ready by polling health endpoint
pub async fn wait_for_server(url: &str, timeout_secs: u64) -> Result<()> {
    let client = Client::new();
    let start = std::time::Instant::now();

    loop {
        if start.elapsed().as_secs() > timeout_secs {
            anyhow::bail!("Server not ready after {} seconds", timeout_secs);
        }

        match client.get(format!("{}/health", url)).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

/// Retry an operation with exponential backoff
pub async fn retry_with_backoff<F, T, Fut>(
    mut operation: F,
    max_attempts: usize,
    initial_delay_ms: u64,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = initial_delay_ms;

    for attempt in 1..=max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt == max_attempts => return Err(e),
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                delay *= 2; // Exponential backoff
            }
        }
    }

    anyhow::bail!("Max retry attempts reached")
}

/// Parse UUID from JSON value
pub fn parse_uuid_from_json(value: &serde_json::Value, key: &str) -> Result<uuid::Uuid> {
    let uuid_str = value[key]
        .as_str()
        .with_context(|| format!("Missing or invalid '{}' field", key))?;

    uuid::Uuid::parse_str(uuid_str).with_context(|| format!("Invalid UUID format for '{}'", key))
}

/// Extract string from JSON value
pub fn get_string_from_json<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .with_context(|| format!("Missing or invalid '{}' field", key))
}

/// Extract boolean from JSON value
pub fn get_bool_from_json(value: &serde_json::Value, key: &str) -> Result<bool> {
    value[key]
        .as_bool()
        .with_context(|| format!("Missing or invalid '{}' field", key))
}

/// Extract i64 from JSON value
pub fn get_i64_from_json(value: &serde_json::Value, key: &str) -> Result<i64> {
    value[key]
        .as_i64()
        .with_context(|| format!("Missing or invalid '{}' field", key))
}

/// Generate unique test identifier
pub fn generate_test_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string()
}

/// Sleep for a duration (useful for async operations to complete)
pub async fn sleep_ms(milliseconds: u64) {
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

/// Format trace ID as hex string
pub fn format_trace_id(bytes: &[u8; 16]) -> String {
    hex::encode(bytes)
}

/// Format span ID as hex string
pub fn format_span_id(bytes: &[u8; 8]) -> String {
    hex::encode(bytes)
}

// ============================================================================
// Assertion Helpers
// ============================================================================

/// Assert that a JSON value contains a specific key
pub fn assert_json_has_key(value: &serde_json::Value, key: &str) -> Result<()> {
    if value.get(key).is_none() {
        anyhow::bail!("JSON missing required key: {}", key);
    }
    Ok(())
}

/// Assert that a JSON value matches expected value at key
pub fn assert_json_eq(
    value: &serde_json::Value,
    key: &str,
    expected: &serde_json::Value,
) -> Result<()> {
    let actual = value
        .get(key)
        .with_context(|| format!("Missing key: {}", key))?;

    if actual != expected {
        anyhow::bail!(
            "Value mismatch at '{}': expected {:?}, got {:?}",
            key,
            expected,
            actual
        );
    }

    Ok(())
}

/// Assert that a string starts with a prefix
pub fn assert_starts_with(value: &str, prefix: &str) -> Result<()> {
    if !value.starts_with(prefix) {
        anyhow::bail!("'{}' does not start with '{}'", value, prefix);
    }
    Ok(())
}

/// Assert that a collection is not empty
pub fn assert_not_empty<T>(collection: &[T], message: &str) -> Result<()> {
    if collection.is_empty() {
        anyhow::bail!("{}", message);
    }
    Ok(())
}
