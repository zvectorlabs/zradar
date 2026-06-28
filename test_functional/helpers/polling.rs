//! Polling utilities for async test assertions.
//!
//! Replaces all `tokio::time::sleep` + immediate-assert patterns with
//! deterministic polling that succeeds as soon as data is available and
//! fails with a clear timeout error if it never arrives.

use anyhow::{Result, bail};
use serde_json::Value;
use std::future::Future;
use std::time::{Duration, Instant};

use crate::helpers::ApiClient;

/// Default poll timeout used by the convenience helpers below.
///
/// Ingest is always WAL-backed (no synchronous direct path), so every
/// read-after-write needs async propagation time: OTLP ack → WAL flush
/// (`ingestor.wal.flush_interval_ms`) → Parquet → query. Polling succeeds as
/// soon as the data lands, so the fast path stays fast; this is only the
/// upper bound before a genuinely-missing result is declared a failure.
///
/// 10s (not a tighter value) because the full suite runs ~170 tests in
/// parallel, all contending on the shared WAL flusher + DataFusion read path;
/// heavier analytics aggregations need headroom under that load.
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// Default interval between poll attempts.
///
/// Kept close to `ingestor.wal.flush_interval_ms` (25ms in the test config):
/// WAL-backed data typically lands ~25–50ms after ack, so a coarse interval
/// would make every read-after-write test idle most of a tick. 50ms catches
/// the flush on the first or second poll while keeping request volume low.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Poll `check` repeatedly until it returns `Ok(Some(T))`, then return that
/// value.  Returns an error if `timeout` elapses without success.
///
/// `check` returns:
/// - `Ok(Some(v))` — done, return `v`
/// - `Ok(None)`    — not ready yet, retry
/// - `Err(e)`      — propagate immediately
pub async fn poll_until<F, Fut, T>(mut check: F, timeout: Duration, interval: Duration) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match check().await? {
            Some(value) => return Ok(value),
            None => {
                if Instant::now() >= deadline {
                    bail!("poll_until: condition not met within {:?}", timeout);
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

/// Poll a REST URL (GET) until the response JSON contains a non-empty `items`
/// array, then return that array.
///
/// Returns an error on HTTP failure or if the timeout elapses.
pub async fn wait_for_items(
    client: &ApiClient,
    url: &str,
    timeout: Duration,
) -> Result<Vec<Value>> {
    poll_until(
        || async {
            let response = client.get(url).await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                bail!("wait_for_items: GET {} returned {}: {}", url, status, body);
            }
            let data: Value = response.json().await?;
            let items = data
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if items.is_empty() {
                Ok(None)
            } else {
                Ok(Some(items))
            }
        },
        timeout,
        DEFAULT_POLL_INTERVAL,
    )
    .await
}

/// Poll `wait_for_items` with the default timeout.
pub async fn wait_for_items_default(client: &ApiClient, url: &str) -> Result<Vec<Value>> {
    wait_for_items(client, url, DEFAULT_POLL_TIMEOUT).await
}

/// Poll `GET /api/v1/traces/{trace_id_hex}` until the
/// response contains at least one span, then return the full trace JSON.
///
/// Returns an error on HTTP 4xx/5xx or if the timeout elapses.
pub async fn wait_for_trace(
    client: &ApiClient,
    trace_id_hex: &str,
    timeout: Duration,
) -> Result<Value> {
    let url = format!("/api/v1/traces/{}", trace_id_hex);
    poll_until(
        || async {
            let response = client.get(&url).await?;
            if !response.status().is_success() {
                return Ok(None);
            }
            let data: Value = response.json().await?;
            let has_spans = data["spans"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if has_spans { Ok(Some(data)) } else { Ok(None) }
        },
        timeout,
        DEFAULT_POLL_INTERVAL,
    )
    .await
}

/// Poll `wait_for_trace` with the default timeout.
pub async fn wait_for_trace_default(client: &ApiClient, trace_id_hex: &str) -> Result<Value> {
    wait_for_trace(client, trace_id_hex, DEFAULT_POLL_TIMEOUT).await
}
