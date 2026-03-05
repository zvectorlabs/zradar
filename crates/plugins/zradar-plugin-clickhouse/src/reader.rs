//! ClickHouse telemetry reader implementation

use clickhouse::Row;
use serde::Deserialize;
use std::sync::Arc;

use crate::client::ClickHouseClient;

/// ClickHouse telemetry reader
pub struct ClickHouseTelemetryReader {
    client: Arc<ClickHouseClient>,
}

impl ClickHouseTelemetryReader {
    /// Create a new reader
    pub fn new(client: Arc<ClickHouseClient>) -> Self {
        Self { client }
    }

    /// Query traces with filters
    pub async fn query_traces(
        &self,
        filters: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let project_id = filters["project_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("project_id required"))?;

        let limit = filters["limit"].as_u64().unwrap_or(100).min(1000);
        let offset = filters["offset"].as_u64().unwrap_or(0);

        // Build query with optional filters
        let mut conditions = vec![format!("project_id = '{}'", project_id)];

        if let Some(start) = filters["start"].as_str() {
            conditions.push(format!("timestamp >= toDateTime('{}')", start));
        }
        if let Some(end) = filters["end"].as_str() {
            conditions.push(format!("timestamp <= toDateTime('{}')", end));
        }
        if let Some(name) = filters["name"].as_str() {
            conditions.push(format!("name LIKE '%{}%'", name));
        }
        if let Some(status) = filters["status"].as_str() {
            conditions.push(format!("status = '{}'", status));
        }

        let where_clause = conditions.join(" AND ");

        let query = format!(
            r#"
            SELECT 
                trace_id,
                min(name) as trace_name,
                min(timestamp) as start_time,
                max(timestamp) as end_time,
                max(duration_ns) / 1000000 as duration_ms,
                anyIf(status, parent_span_id = '') as status,
                count() as span_count,
                countIf(status = 'error') as error_count
            FROM spans
            WHERE {}
            GROUP BY trace_id
            ORDER BY start_time DESC
            LIMIT {} OFFSET {}
            "#,
            where_clause, limit, offset
        );

        #[derive(Row, Deserialize)]
        struct TraceRow {
            trace_id: String,
            trace_name: String,
            start_time: u32,
            end_time: u32,
            duration_ms: i64,
            status: String,
            span_count: u32,
            error_count: u32,
        }

        let rows: Vec<TraceRow> = self.client.client().query(&query).fetch_all().await?;

        let traces: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "trace_id": r.trace_id,
                    "name": r.trace_name,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "duration_ms": r.duration_ms,
                    "status": r.status,
                    "span_count": r.span_count,
                    "error_count": r.error_count,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "items": traces,
            "total": traces.len(),
            "limit": limit,
            "offset": offset,
        }))
    }

    /// Get trace detail
    pub async fn get_trace_detail(
        &self,
        project_id: uuid::Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let query = format!(
            r#"
            SELECT *
            FROM spans
            WHERE project_id = '{}' AND trace_id = '{}'
            ORDER BY timestamp ASC
            "#,
            project_id, trace_id
        );

        #[derive(Row, Deserialize)]
        struct SpanRow {
            span_id: String,
            trace_id: String,
            parent_span_id: String,
            name: String,
            kind: String,
            timestamp: u32,
            duration_ns: i64,
            status: String,
            status_message: String,
            attributes: String,
        }

        let rows: Vec<SpanRow> = self.client.client().query(&query).fetch_all().await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let spans: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "span_id": r.span_id,
                    "trace_id": r.trace_id,
                    "parent_span_id": r.parent_span_id,
                    "name": r.name,
                    "kind": r.kind,
                    "timestamp": r.timestamp,
                    "duration_ns": r.duration_ns,
                    "status": r.status,
                    "status_message": r.status_message,
                    "attributes": r.attributes,
                })
            })
            .collect();

        Ok(Some(serde_json::json!({
            "trace_id": trace_id,
            "project_id": project_id.to_string(),
            "spans": spans,
            "span_count": spans.len(),
        })))
    }

    /// Query spans
    pub async fn query_spans(
        &self,
        filters: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let project_id = filters["project_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("project_id required"))?;

        let limit = filters["limit"].as_u64().unwrap_or(100).min(1000);

        let query = format!(
            "SELECT * FROM spans WHERE project_id = '{}' ORDER BY timestamp DESC LIMIT {}",
            project_id, limit
        );

        #[derive(Row, Deserialize)]
        struct SpanRow {
            span_id: String,
            trace_id: String,
            name: String,
            timestamp: u32,
            duration_ns: i64,
            status: String,
        }

        let rows: Vec<SpanRow> = self.client.client().query(&query).fetch_all().await?;

        let spans: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "span_id": r.span_id,
                    "trace_id": r.trace_id,
                    "name": r.name,
                    "timestamp": r.timestamp,
                    "duration_ns": r.duration_ns,
                    "status": r.status,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "items": spans,
            "total": spans.len(),
        }))
    }

    /// Get analytics
    pub async fn get_analytics(
        &self,
        query: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let project_id = query["project_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("project_id required"))?;

        let sql = format!(
            r#"
            SELECT 
                toStartOfHour(timestamp) as time_bucket,
                count() as request_count,
                avg(duration_ns) / 1000000 as avg_duration_ms,
                quantile(0.95)(duration_ns) / 1000000 as p95_duration_ms,
                countIf(status = 'error') as error_count
            FROM spans
            WHERE project_id = '{}'
            GROUP BY time_bucket
            ORDER BY time_bucket DESC
            LIMIT 168
            "#,
            project_id
        );

        #[derive(Row, Deserialize)]
        struct AnalyticsRow {
            time_bucket: u32,
            request_count: u64,
            avg_duration_ms: f64,
            p95_duration_ms: f64,
            error_count: u64,
        }

        let rows: Vec<AnalyticsRow> = self.client.client().query(&sql).fetch_all().await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "time_bucket": r.time_bucket,
                    "request_count": r.request_count,
                    "avg_duration_ms": r.avg_duration_ms,
                    "p95_duration_ms": r.p95_duration_ms,
                    "error_count": r.error_count,
                })
            })
            .collect();

        Ok(serde_json::json!({ "results": results }))
    }
}

use async_trait::async_trait;
use uuid::Uuid;
use zradar_traits::repositories::telemetry::{AnalyticsReader, MetricsSummary, TimeSeriesPoint};

#[async_trait]
impl AnalyticsReader for ClickHouseTelemetryReader {
    async fn get_daily_trace_counts(
        &self,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<TimeSeriesPoint>> {
        let sql = format!(
            r#"
            SELECT
                formatDateTime(toStartOfDay(timestamp), '%Y-%m-%dT%H:%M:%S.000Z') as timestamp,
                count() as value
            FROM spans
            WHERE project_id = '{}'
              AND timestamp >= toDateTime({})
              AND timestamp <= toDateTime({})
              AND parent_span_id = '' -- Only count root spans (traces)
            GROUP BY timestamp
            ORDER BY timestamp ASC
            "#,
            project_id,
            start / 1000,
            end / 1000
        );

        #[derive(Row, Deserialize)]
        struct PointRow {
            timestamp: String,
            value: u64,
        }

        let rows: Vec<PointRow> = self.client.client().query(&sql).fetch_all().await?;

        Ok(rows
            .into_iter()
            .map(|r| TimeSeriesPoint {
                timestamp: r.timestamp,
                value: r.value as f64,
            })
            .collect())
    }

    async fn get_metrics_summary(
        &self,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<MetricsSummary> {
        let sql = format!(
            r#"
            SELECT
                count() as total_traces,
                countIf(status = 'error') / count() as error_rate,
                quantile(0.5)(duration_ns) / 1000000 as p50_latency,
                quantile(0.9)(duration_ns) / 1000000 as p90_latency,
                quantile(0.99)(duration_ns) / 1000000 as p99_latency
            FROM spans
            WHERE project_id = '{}'
              AND timestamp >= toDateTime({})
              AND timestamp <= toDateTime({})
              AND parent_span_id = ''
            "#,
            project_id,
            start / 1000,
            end / 1000
        );

        #[derive(Row, Deserialize)]
        struct SummaryRow {
            total_traces: u64,
            error_rate: f64,
            p50_latency: f64,
            p90_latency: f64,
            p99_latency: f64,
        }

        let row: Option<SummaryRow> = self.client.client().query(&sql).fetch_optional().await?;

        if let Some(r) = row {
            Ok(MetricsSummary {
                total_traces: r.total_traces as i64,
                error_rate: r.error_rate,
                p50_latency: r.p50_latency,
                p90_latency: r.p90_latency,
                p99_latency: r.p99_latency,
            })
        } else {
            Ok(MetricsSummary::default())
        }
    }
}
