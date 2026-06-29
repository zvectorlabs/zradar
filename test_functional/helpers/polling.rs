//! Polling utilities for async test assertions.

use anyhow::{Result, bail};
use serde_json::Value;
use std::future::Future;
use std::time::{Duration, Instant};

use crate::helpers::TransportApiClient;

pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

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

pub async fn wait_for_items(
    client: &TransportApiClient,
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

pub async fn wait_for_items_default(client: &TransportApiClient, url: &str) -> Result<Vec<Value>> {
    wait_for_items(client, url, DEFAULT_POLL_TIMEOUT).await
}

pub async fn wait_for_trace(
    client: &TransportApiClient,
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

pub async fn wait_for_trace_default(
    client: &TransportApiClient,
    trace_id_hex: &str,
) -> Result<Value> {
    wait_for_trace(client, trace_id_hex, DEFAULT_POLL_TIMEOUT).await
}
