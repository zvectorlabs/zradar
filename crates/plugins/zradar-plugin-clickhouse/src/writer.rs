//! ClickHouse telemetry writer implementation

use async_trait::async_trait;
use std::sync::Arc;
use zradar_models::{Metric, Span};
use zradar_traits::TelemetryWriter;

use crate::client::ClickHouseClient;

/// ClickHouse telemetry writer
pub struct ClickHouseTelemetryWriter {
    client: Arc<ClickHouseClient>,
}

impl ClickHouseTelemetryWriter {
    /// Create a new writer
    pub fn new(client: Arc<ClickHouseClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TelemetryWriter for ClickHouseTelemetryWriter {
    /// Insert spans into ClickHouse
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        // Debug logging in test mode
        if std::env::var("ZRADAR_TEST_MODE").is_ok() {
            for span in spans {
                tracing::debug!(
                    span_id = %span.span_id,
                    trace_id = %span.trace_id,
                    span_name = %span.span_name,
                    "Inserting span"
                );
            }
        }

        let mut insert = self.client.client().insert("spans")?;
        for span in spans {
            insert.write(span).await?;
        }
        insert.end().await?;

        tracing::info!(count = spans.len(), "Inserted spans into ClickHouse");

        // Test mode: force synchronous visibility
        if self.client.is_test_mode() {
            let optimize_query = "OPTIMIZE TABLE spans FINAL SETTINGS mutations_sync=2";
            self.client.client().query(optimize_query).execute().await?;
            tracing::debug!("Applied OPTIMIZE TABLE FINAL (test mode)");

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Insert metrics into ClickHouse
    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let mut insert = self.client.client().insert("metrics")?;
        for metric in metrics {
            insert.write(metric).await?;
        }
        insert.end().await?;

        tracing::info!(count = metrics.len(), "Inserted metrics into ClickHouse");

        // Test mode: force synchronous visibility
        if self.client.is_test_mode() {
            let optimize_query = "OPTIMIZE TABLE metrics FINAL SETTINGS mutations_sync=2";
            self.client.client().query(optimize_query).execute().await?;
        }

        Ok(())
    }
}
