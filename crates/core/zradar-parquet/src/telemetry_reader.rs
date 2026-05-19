//! `TelemetryReader` implementation backed by Parquet / DataFusion.
//!
//! All query filters that were broken in the Postgres implementation are fixed
//! here: `status`, `min_duration_ms`, `max_duration_ms`, `time_range` on span
//! search, and accurate pagination totals.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;
use zradar_models::{FileListFilter, LogRecord, Metric, Span};

use zradar_traits::{
    AnalyticsDataPoint, AnalyticsQueryFilters, AnalyticsReader, LogQueryFilters, MetricPoint,
    MetricQueryFilters, MetricSeriesFilters, MetricsSummary, PaginatedResponse, Pagination,
    SpanQueryFilters, TelemetryReader, TimeSeriesPoint, TraceQueryFilters, TraceSummary,
};

use crate::reader::ParquetFileReader;
use crate::schema::logs::record_batch_to_logs;
use crate::schema::metrics::record_batch_to_metrics;
use crate::schema::spans::record_batch_to_spans;

/// Implements `TelemetryReader` using DataFusion SQL over Parquet files.
pub struct ParquetTelemetryReader {
    reader: Arc<ParquetFileReader>,
}

impl ParquetTelemetryReader {
    /// Create a new reader.
    pub fn new(reader: Arc<ParquetFileReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl AnalyticsReader for ParquetTelemetryReader {
    async fn get_daily_trace_counts(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<TimeSeriesPoint>> {
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(project_id),
            signal_type: Some("traces".to_string()),
            time_range_start: Some(start / 1_000),
            time_range_end: Some(end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        // Bucket by day (86_400_000_000_000 ns per day), count distinct trace_ids.
        let day_ns: i64 = 86_400_000_000_000;
        let sql = format!(
            r#"SELECT
                (MIN(timestamp) / {day_ns}) * {day_ns} AS day_bucket,
                COUNT(DISTINCT trace_id) AS cnt
            FROM spans
            WHERE project_id = '{project_id}'
              AND timestamp >= {start}
              AND timestamp <= {end}
            GROUP BY (timestamp / {day_ns})
            ORDER BY day_bucket"#
        );

        let batches = self
            .reader
            .query_parquet(file_filter, &sql)
            .await
            .context("Failed to query daily trace counts")?;

        let mut points = Vec::new();
        for batch in &batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let bucket_col = batch
                .column_by_name("day_bucket")
                .ok_or_else(|| anyhow!("missing column: day_bucket"))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("day_bucket is not Int64Array"))?;
            let cnt_col = extract_count_col(batch)?;

            for i in 0..n {
                // Convert nanoseconds → RFC3339 date string
                let ts_secs = bucket_col.value(i) / 1_000_000_000;
                let dt = chrono::DateTime::from_timestamp(ts_secs, 0)
                    .unwrap_or_default()
                    .to_rfc3339();
                points.push(TimeSeriesPoint {
                    timestamp: dt,
                    value: cnt_col.value(i) as f64,
                });
            }
        }

        Ok(points)
    }

    async fn get_metrics_summary(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<MetricsSummary> {
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(project_id),
            signal_type: Some("traces".to_string()),
            time_range_start: Some(start / 1_000),
            time_range_end: Some(end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        let sql = format!(
            r#"SELECT
                COUNT(DISTINCT trace_id)                                              AS total_traces,
                SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)               AS error_count,
                approx_percentile_cont(CAST(duration_ns AS DOUBLE), 0.50) / 1000000.0 AS p50_ms,
                approx_percentile_cont(CAST(duration_ns AS DOUBLE), 0.90) / 1000000.0 AS p90_ms,
                approx_percentile_cont(CAST(duration_ns AS DOUBLE), 0.99) / 1000000.0 AS p99_ms
            FROM spans
            WHERE project_id = '{project_id}'
              AND parent_span_id = ''
              AND timestamp >= {start}
              AND timestamp <= {end}"#
        );

        let batches = self
            .reader
            .query_parquet(file_filter, &sql)
            .await
            .context("Failed to query metrics summary")?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(MetricsSummary::default());
        }

        let batch = &batches[0];
        let total_traces = batch
            .column_by_name("total_traces")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0);

        let error_count = batch
            .column_by_name("error_count")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0);

        let p50 = batch
            .column_by_name("p50_ms")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0.0);

        let p90 = batch
            .column_by_name("p90_ms")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0.0);

        let p99 = batch
            .column_by_name("p99_ms")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0.0);

        let error_rate = if total_traces > 0 {
            error_count as f64 / total_traces as f64
        } else {
            0.0
        };

        Ok(MetricsSummary {
            total_traces,
            error_rate,
            p50_latency: p50,
            p90_latency: p90,
            p99_latency: p99,
        })
    }

    async fn query_analytics(
        &self,
        tenant_id: Uuid,
        filters: AnalyticsQueryFilters,
    ) -> anyhow::Result<Vec<AnalyticsDataPoint>> {
        // Whitelisted metric names and their SQL aggregation expressions.
        const ALLOWED_METRICS: &[(&str, &str)] = &[
            ("trace_count", "COUNT(DISTINCT trace_id)"),
            ("span_count", "COUNT(*)"),
            ("total_tokens", "CAST(SUM(total_tokens) AS DOUBLE)"),
            ("prompt_tokens", "CAST(SUM(prompt_tokens) AS DOUBLE)"),
            (
                "completion_tokens",
                "CAST(SUM(completion_tokens) AS DOUBLE)",
            ),
            ("total_cost_usd", "SUM(total_cost_usd)"),
            (
                "avg_duration_ms",
                "AVG(CAST(duration_ns AS DOUBLE)) / 1000000.0",
            ),
            (
                "error_count",
                "CAST(SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) AS DOUBLE)",
            ),
        ];

        // Whitelisted column names that can be used in GROUP BY and WHERE filters.
        const ALLOWED_DIMENSIONS: &[&str] = &[
            "agent_name",
            "llm_model",
            "session_id",
            "service_name",
            "span_name",
            "span_type",
            "status_code",
            "agent_type",
        ];

        // 1. Resolve metric → SQL aggregation expression
        let agg_expr = ALLOWED_METRICS
            .iter()
            .find(|(name, _)| *name == filters.metric.as_str())
            .map(|(_, expr)| *expr)
            .ok_or_else(|| anyhow!("unsupported metric: {}", filters.metric))?;

        // 2. Validate group_by dimensions
        for g in &filters.group_by {
            if !ALLOWED_DIMENSIONS.contains(&g.as_str()) {
                return Err(anyhow!("unsupported group_by dimension: {}", g));
            }
        }

        // 3. Build file filter
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(filters.project_id),
            signal_type: Some("traces".to_string()),
            time_range_start: Some(filters.start / 1_000),
            time_range_end: Some(filters.end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        // 4. Build WHERE conditions
        let day_ns: i64 = 86_400_000_000_000;
        let mut conditions = vec![
            format!("project_id = '{}'", filters.project_id),
            format!("timestamp >= {}", filters.start),
            format!("timestamp <= {}", filters.end),
        ];
        // Exclude empty strings for group-by columns so we only see real data
        for g in &filters.group_by {
            conditions.push(format!("{} != ''", g));
        }
        // Apply ad-hoc WHERE filters
        for (k, v) in &filters.filters {
            if ALLOWED_DIMENSIONS.contains(&k.as_str()) {
                conditions.push(format!("{} = '{}'", k, escape_sql_str(v)));
            }
        }
        let where_clause = conditions.join(" AND ");

        // 5. Build SELECT / GROUP BY / ORDER BY
        let group_cols_csv = filters.group_by.join(", ");
        let select_groups = if group_cols_csv.is_empty() {
            String::new()
        } else {
            format!(", {}", group_cols_csv)
        };
        let group_clause = if group_cols_csv.is_empty() {
            "bucket".to_string()
        } else {
            format!("bucket, {}", group_cols_csv)
        };

        let sql = format!(
            r#"SELECT
                (timestamp / {day_ns}) * {day_ns} AS bucket
                {select_groups},
                {agg_expr} AS value
            FROM spans
            WHERE {where_clause}
            GROUP BY {group_clause}
            ORDER BY bucket"#
        );

        // 6. Execute
        let batches = self
            .reader
            .query_parquet(file_filter, &sql)
            .await
            .context("Failed to execute analytics query")?;

        // 7. Convert RecordBatches → Vec<AnalyticsDataPoint>
        let mut results = Vec::new();
        for batch in &batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }

            let bucket_col = batch
                .column_by_name("bucket")
                .ok_or_else(|| anyhow!("missing column: bucket"))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("bucket is not Int64Array"))?;

            // Handle both Float64 (SUM/AVG) and Int64 (COUNT) result types
            let value_arr = batch
                .column_by_name("value")
                .ok_or_else(|| anyhow!("missing column: value"))?;

            let value_f64 = value_arr.as_any().downcast_ref::<Float64Array>();
            let value_i64 = value_arr.as_any().downcast_ref::<Int64Array>();

            // Collect group-by StringArray columns
            let group_arrays: Vec<(&str, &StringArray)> = filters
                .group_by
                .iter()
                .filter_map(|g| {
                    batch
                        .column_by_name(g)
                        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                        .map(|arr| (g.as_str(), arr))
                })
                .collect();

            for i in 0..n {
                let value = if let Some(f64_col) = value_f64 {
                    f64_col.value(i)
                } else if let Some(i64_col) = value_i64 {
                    i64_col.value(i) as f64
                } else {
                    0.0
                };

                let mut groups = HashMap::new();
                for (name, arr) in &group_arrays {
                    groups.insert(name.to_string(), arr.value(i).to_string());
                }

                results.push(AnalyticsDataPoint {
                    bucket_ts: bucket_col.value(i),
                    value,
                    groups,
                });
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl TelemetryReader for ParquetTelemetryReader {
    // -------------------------------------------------------------------------
    // query_traces
    // -------------------------------------------------------------------------

    async fn query_traces(
        &self,
        filters: TraceQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<TraceSummary>> {
        let project_id = filters
            .project_id
            .ok_or_else(|| anyhow!("project_id is required for query_traces"))?;

        let (limit, offset) = pagination_values(&filters.pagination);
        let file_filter = trace_file_filter(project_id, &filters);

        // ------------------------------------------------------------------
        // Inner aggregate query: group spans into trace summaries.
        // ------------------------------------------------------------------
        let mut inner_where = format!("project_id = '{}'", project_id);
        if let Some(ref svc) = filters.service_name {
            inner_where.push_str(&format!(" AND service_name = '{}'", escape_sql_str(svc)));
        }
        if let Some(ref tr) = filters.time_range {
            // timestamp is in nanoseconds; filter on raw ns.
            inner_where.push_str(&format!(" AND timestamp >= {}", tr.start));
            inner_where.push_str(&format!(" AND timestamp <= {}", tr.end));
        }
        // Agent attribute filters — applied at span level before aggregation
        if let Some(ref llm_model) = filters.llm_model {
            inner_where.push_str(&format!(" AND llm_model = '{}'", escape_sql_str(llm_model)));
        }
        if let Some(ref agent_name) = filters.agent_name {
            inner_where.push_str(&format!(
                " AND agent_name = '{}'",
                escape_sql_str(agent_name)
            ));
        }
        if let Some(ref session_id) = filters.session_id {
            inner_where.push_str(&format!(
                " AND session_id = '{}'",
                escape_sql_str(session_id)
            ));
        }

        let inner_sql = format!(
            r#"SELECT
                trace_id,
                MIN(span_name)  AS trace_name,
                MIN(timestamp)  AS start_time,
                MAX(timestamp + duration_ns) AS end_time,
                (MAX(timestamp + duration_ns) - MIN(timestamp)) / 1000000 AS duration_ms,
                COUNT(*)        AS span_count,
                MIN(service_name) AS service_name,
                MAX(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) AS has_error
            FROM spans
            WHERE {inner_where}
            GROUP BY trace_id"#
        );

        // ------------------------------------------------------------------
        // Outer query: apply status / duration filters as HAVING equivalents.
        // ------------------------------------------------------------------
        let mut outer_conditions: Vec<String> = Vec::new();
        if let Some(ref status) = filters.status {
            match status.to_uppercase().as_str() {
                "ERROR" => outer_conditions.push("has_error = 1".to_string()),
                "OK" => outer_conditions.push("has_error = 0".to_string()),
                _ => {}
            }
        }
        if let Some(min_ms) = filters.min_duration_ms {
            outer_conditions.push(format!("duration_ms >= {min_ms}"));
        }
        if let Some(max_ms) = filters.max_duration_ms {
            outer_conditions.push(format!("duration_ms <= {max_ms}"));
        }

        let outer_where = if outer_conditions.is_empty() {
            "1=1".to_string()
        } else {
            outer_conditions.join(" AND ")
        };

        // Count query (for pagination total).
        let count_sql = format!("SELECT COUNT(*) AS cnt FROM ({inner_sql}) t WHERE {outer_where}");
        let count_batches = self
            .reader
            .query_parquet(file_filter.clone(), &count_sql)
            .await
            .context("Failed to count traces")?;
        let total = extract_count(&count_batches).unwrap_or(0);

        // Main query with ORDER BY + LIMIT/OFFSET.
        let main_sql = format!(
            r#"SELECT * FROM ({inner_sql}) t
            WHERE {outer_where}
            ORDER BY start_time DESC
            LIMIT {limit} OFFSET {offset}"#
        );
        let batches = self
            .reader
            .query_parquet(file_filter, &main_sql)
            .await
            .context("Failed to query traces")?;

        let summaries = batches_to_trace_summaries(&batches)?;

        Ok(PaginatedResponse {
            items: summaries,
            total,
            limit,
            offset,
        })
    }

    // -------------------------------------------------------------------------
    // get_trace_detail
    // -------------------------------------------------------------------------

    async fn get_trace_detail(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<Vec<Span>>> {
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(project_id),
            signal_type: Some("traces".to_string()),
            deleted: Some(false),
            ..Default::default()
        };

        let sql = format!(
            "SELECT * FROM spans WHERE tenant_id = '{}' AND project_id = '{}' AND trace_id = '{}' ORDER BY timestamp",
            tenant_id,
            project_id,
            escape_sql_str(trace_id)
        );

        let batches = self
            .reader
            .query_parquet(file_filter, &sql)
            .await
            .context("Failed to query trace detail")?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(None);
        }

        let mut spans = Vec::with_capacity(total_rows);
        for batch in &batches {
            spans.extend(record_batch_to_spans(batch)?);
        }
        Ok(Some(spans))
    }

    // -------------------------------------------------------------------------
    // query_spans
    // -------------------------------------------------------------------------

    async fn query_spans(
        &self,
        filters: SpanQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<Span>> {
        let project_id = filters
            .project_id
            .ok_or_else(|| anyhow!("project_id is required for query_spans"))?;

        let (limit, offset) = pagination_values(&filters.pagination);

        let file_filter = span_file_filter(project_id, &filters);

        // Build WHERE clause.
        let mut conditions = vec![format!("project_id = '{project_id}'")];

        if let Some(ref tr) = filters.trace_id {
            conditions.push(format!("trace_id = '{}'", escape_sql_str(tr)));
        }
        if let Some(ref svc) = filters.service_name {
            conditions.push(format!("service_name = '{}'", escape_sql_str(svc)));
        }
        if let Some(ref name) = filters.span_name {
            conditions.push(format!("span_name LIKE '%{}%'", escape_sql_str(name)));
        }
        if let Some(types) = filters.span_types.as_ref().filter(|t| !t.is_empty()) {
            let list: Vec<String> = types
                .iter()
                .map(|t| format!("'{}'", escape_sql_str(t)))
                .collect();
            conditions.push(format!("span_type IN ({})", list.join(", ")));
        }
        if let Some(ref status) = filters.status {
            conditions.push(format!("status_code = '{}'", escape_sql_str(status)));
        }
        if let Some(ref tr) = filters.time_range {
            // Fix: apply time_range filter at file_list level AND as SQL predicate.
            conditions.push(format!("timestamp >= {}", tr.start));
            conditions.push(format!("timestamp <= {}", tr.end));
        }
        // Agent attribute filters
        if let Some(ref llm_model) = filters.llm_model {
            conditions.push(format!("llm_model = '{}'", escape_sql_str(llm_model)));
        }
        if let Some(ref agent_name) = filters.agent_name {
            conditions.push(format!("agent_name = '{}'", escape_sql_str(agent_name)));
        }
        if let Some(ref session_id) = filters.session_id {
            conditions.push(format!("session_id = '{}'", escape_sql_str(session_id)));
        }

        let where_clause = conditions.join(" AND ");

        let count_sql = format!("SELECT COUNT(*) AS cnt FROM spans WHERE {where_clause}");
        let count_batches = self
            .reader
            .query_parquet(file_filter.clone(), &count_sql)
            .await
            .context("Failed to count spans")?;
        let total = extract_count(&count_batches).unwrap_or(0);

        let main_sql = format!(
            "SELECT * FROM spans WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        let batches = self
            .reader
            .query_parquet(file_filter, &main_sql)
            .await
            .context("Failed to query spans")?;

        let mut spans = Vec::new();
        for batch in &batches {
            spans.extend(record_batch_to_spans(batch)?);
        }

        Ok(PaginatedResponse {
            items: spans,
            total,
            limit,
            offset,
        })
    }

    // -------------------------------------------------------------------------
    // get_span
    // -------------------------------------------------------------------------

    async fn get_span(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        span_id: &str,
    ) -> anyhow::Result<Option<Span>> {
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(project_id),
            signal_type: Some("traces".to_string()),
            deleted: Some(false),
            ..Default::default()
        };

        let sql = format!(
            "SELECT * FROM spans WHERE tenant_id = '{}' AND project_id = '{}' AND span_id = '{}' LIMIT 1",
            tenant_id,
            project_id,
            escape_sql_str(span_id)
        );

        let batches = self
            .reader
            .query_parquet(file_filter, &sql)
            .await
            .context("Failed to query span by ID")?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(None);
        }

        let spans = record_batch_to_spans(&batches[0])?;
        Ok(spans.into_iter().next())
    }

    // -------------------------------------------------------------------------
    // query_logs
    // -------------------------------------------------------------------------

    async fn query_logs(
        &self,
        filters: LogQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<LogRecord>> {
        let project_id = filters
            .project_id
            .ok_or_else(|| anyhow!("project_id is required for query_logs"))?;

        let (limit, offset) = pagination_values(&filters.pagination);

        let file_filter = FileListFilter {
            project_id: Some(project_id),
            signal_type: Some("logs".to_string()),
            time_range_start: filters.time_range.as_ref().map(|tr| tr.start / 1_000),
            time_range_end: filters.time_range.as_ref().map(|tr| tr.end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        let mut conditions = vec![format!("project_id = '{project_id}'")];

        if let Some(ref sev) = filters.severity {
            conditions.push(format!("severity = '{}'", escape_sql_str(sev)));
        }
        if let Some(ref svc) = filters.service_name {
            conditions.push(format!("service_name = '{}'", escape_sql_str(svc)));
        }
        if let Some(ref tid) = filters.trace_id {
            conditions.push(format!("trace_id = '{}'", escape_sql_str(tid)));
        }
        if let Some(ref text) = filters.search_text {
            conditions.push(format!("message LIKE '%{}%'", escape_sql_str(text)));
        }
        if let Some(ref agent) = filters.agent_name {
            conditions.push(format!("agent_name = '{}'", escape_sql_str(agent)));
        }
        if let Some(ref sess) = filters.session_id {
            conditions.push(format!("session_id = '{}'", escape_sql_str(sess)));
        }
        if let Some(ref tr) = filters.time_range {
            conditions.push(format!("timestamp >= {}", tr.start));
            conditions.push(format!("timestamp <= {}", tr.end));
        }

        let where_clause = conditions.join(" AND ");

        let count_sql = format!("SELECT COUNT(*) AS cnt FROM logs WHERE {where_clause}");
        let count_batches = self
            .reader
            .query_parquet_as(file_filter.clone(), "logs", &count_sql)
            .await
            .context("Failed to count logs")?;
        let total = extract_count(&count_batches).unwrap_or(0);

        let main_sql = format!(
            "SELECT * FROM logs WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        let batches = self
            .reader
            .query_parquet_as(file_filter, "logs", &main_sql)
            .await
            .context("Failed to query logs")?;

        let mut logs = Vec::new();
        for batch in &batches {
            logs.extend(record_batch_to_logs(batch)?);
        }

        Ok(PaginatedResponse {
            items: logs,
            total,
            limit,
            offset,
        })
    }

    // -------------------------------------------------------------------------
    // get_log
    // -------------------------------------------------------------------------

    async fn get_log(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        log_id: &str,
    ) -> anyhow::Result<Option<LogRecord>> {
        let file_filter = FileListFilter {
            tenant_id: Some(tenant_id),
            project_id: Some(project_id),
            signal_type: Some("logs".to_string()),
            deleted: Some(false),
            ..Default::default()
        };

        let sql = format!(
            "SELECT * FROM logs WHERE tenant_id = '{}' AND project_id = '{}' AND id = '{}' LIMIT 1",
            tenant_id,
            project_id,
            escape_sql_str(log_id)
        );

        let batches = self
            .reader
            .query_parquet_as(file_filter, "logs", &sql)
            .await
            .context("Failed to query log by ID")?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(None);
        }

        let logs = record_batch_to_logs(&batches[0])?;
        Ok(logs.into_iter().next())
    }

    // -------------------------------------------------------------------------
    // query_metrics
    // -------------------------------------------------------------------------

    async fn query_metrics(
        &self,
        filters: MetricQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<Metric>> {
        let project_id = filters
            .project_id
            .ok_or_else(|| anyhow!("project_id is required for query_metrics"))?;

        let (limit, offset) = pagination_values(&filters.pagination);

        let file_filter = FileListFilter {
            project_id: Some(project_id),
            signal_type: Some("metrics".to_string()),
            time_range_start: filters.time_range.as_ref().map(|tr| tr.start / 1_000),
            time_range_end: filters.time_range.as_ref().map(|tr| tr.end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        let mut conditions = vec![format!("project_id = '{project_id}'")];

        if let Some(ref name) = filters.metric_name {
            conditions.push(format!("metric_name = '{}'", escape_sql_str(name)));
        }
        if let Some(ref svc) = filters.service_name {
            conditions.push(format!("service_name = '{}'", escape_sql_str(svc)));
        }
        if let Some(ref agent) = filters.agent_name {
            conditions.push(format!("agent_name = '{}'", escape_sql_str(agent)));
        }
        if let Some(ref tr) = filters.time_range {
            conditions.push(format!("timestamp >= {}", tr.start));
            conditions.push(format!("timestamp <= {}", tr.end));
        }

        let where_clause = conditions.join(" AND ");

        let count_sql = format!("SELECT COUNT(*) AS cnt FROM metrics WHERE {where_clause}");
        let count_batches = self
            .reader
            .query_parquet_as(file_filter.clone(), "metrics", &count_sql)
            .await
            .context("Failed to count metrics")?;
        let total = extract_count(&count_batches).unwrap_or(0);

        let main_sql = format!(
            "SELECT * FROM metrics WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        let batches = self
            .reader
            .query_parquet_as(file_filter, "metrics", &main_sql)
            .await
            .context("Failed to query metrics")?;

        let mut metrics = Vec::new();
        for batch in &batches {
            metrics.extend(record_batch_to_metrics(batch)?);
        }

        Ok(PaginatedResponse {
            items: metrics,
            total,
            limit,
            offset,
        })
    }

    // -------------------------------------------------------------------------
    // query_metric_series
    // -------------------------------------------------------------------------

    async fn query_metric_series(
        &self,
        filters: MetricSeriesFilters,
    ) -> anyhow::Result<Vec<MetricPoint>> {
        let project_id = filters
            .project_id
            .ok_or_else(|| anyhow!("project_id is required for query_metric_series"))?;

        let file_filter = FileListFilter {
            project_id: Some(project_id),
            signal_type: Some("metrics".to_string()),
            time_range_start: filters.time_range.as_ref().map(|tr| tr.start / 1_000),
            time_range_end: filters.time_range.as_ref().map(|tr| tr.end / 1_000),
            deleted: Some(false),
            ..Default::default()
        };

        // Interval in nanoseconds for timestamp bucketing.
        let interval_ns = (filters.interval_seconds as i64) * 1_000_000_000_i64;

        let agg_fn = match filters.aggregation.to_lowercase().as_str() {
            "sum" => "SUM(value)",
            "min" => "MIN(value)",
            "max" => "MAX(value)",
            "count" => "COUNT(*)",
            _ => "AVG(value)", // default: avg
        };

        let mut conditions = vec![
            format!("project_id = '{project_id}'"),
            format!("metric_name = '{}'", escape_sql_str(&filters.metric_name)),
        ];
        if let Some(ref svc) = filters.service_name {
            conditions.push(format!("service_name = '{}'", escape_sql_str(svc)));
        }
        if let Some(ref tr) = filters.time_range {
            conditions.push(format!("timestamp >= {}", tr.start));
            conditions.push(format!("timestamp <= {}", tr.end));
        }
        let where_clause = conditions.join(" AND ");

        let sql = format!(
            r#"SELECT
                (timestamp / {interval_ns}) * {interval_ns} AS bucket_ts,
                {agg_fn} AS value
            FROM metrics
            WHERE {where_clause}
            GROUP BY bucket_ts
            ORDER BY bucket_ts"#
        );

        let batches = self
            .reader
            .query_parquet_as(file_filter, "metrics", &sql)
            .await
            .context("Failed to query metric series")?;

        let mut points = Vec::new();
        for batch in &batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let bucket_col = batch
                .column_by_name("bucket_ts")
                .ok_or_else(|| anyhow!("missing column: bucket_ts"))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("bucket_ts is not Int64Array"))?;
            let value_col = batch
                .column_by_name("value")
                .ok_or_else(|| anyhow!("missing column: value"))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("value is not Float64Array"))?;

            for i in 0..n {
                points.push(MetricPoint {
                    bucket_ts: bucket_col.value(i),
                    value: value_col.value(i),
                });
            }
        }

        Ok(points)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Default pagination: limit 50, offset 0.
fn pagination_values(p: &Pagination) -> (u32, u32) {
    (p.limit.unwrap_or(50), p.offset.unwrap_or(0))
}

/// Build a `FileListFilter` for trace queries.
fn trace_file_filter(project_id: Uuid, filters: &TraceQueryFilters) -> FileListFilter {
    FileListFilter {
        project_id: Some(project_id),
        signal_type: Some("traces".to_string()),
        time_range_start: filters.time_range.as_ref().map(|tr| tr.start / 1_000),
        time_range_end: filters.time_range.as_ref().map(|tr| tr.end / 1_000),
        deleted: Some(false),
        ..Default::default()
    }
}

/// Build a `FileListFilter` for span queries.
fn span_file_filter(project_id: Uuid, filters: &SpanQueryFilters) -> FileListFilter {
    FileListFilter {
        project_id: Some(project_id),
        signal_type: Some("traces".to_string()),
        time_range_start: filters.time_range.as_ref().map(|tr| tr.start / 1_000),
        time_range_end: filters.time_range.as_ref().map(|tr| tr.end / 1_000),
        deleted: Some(false),
        ..Default::default()
    }
}

/// Extract a `COUNT(*)` scalar from a set of `RecordBatch`es.
fn extract_count(batches: &[RecordBatch]) -> Option<u64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Try both "cnt" (our alias) and the first column.
        let col = batch
            .column_by_name("cnt")
            .or_else(|| batch.columns().first())?;

        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(arr.value(0) as u64);
        }
    }
    Some(0)
}

/// Extract a `cnt` Int64Array column from a single RecordBatch.
fn extract_count_col(batch: &RecordBatch) -> anyhow::Result<&Int64Array> {
    batch
        .column_by_name("cnt")
        .ok_or_else(|| anyhow!("missing column: cnt"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("cnt is not Int64Array"))
}

/// Convert `RecordBatch`es returned by the aggregation query into `TraceSummary`.
fn batches_to_trace_summaries(batches: &[RecordBatch]) -> anyhow::Result<Vec<TraceSummary>> {
    let mut out = Vec::new();
    for batch in batches {
        out.extend(batch_to_trace_summaries(batch)?);
    }
    Ok(out)
}

fn batch_to_trace_summaries(batch: &RecordBatch) -> anyhow::Result<Vec<TraceSummary>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(vec![]);
    }

    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("{} is not StringArray", $name))?
        };
    }

    macro_rules! i64_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("{} is not Int64Array", $name))?
        };
    }

    let trace_id_col = str_col!("trace_id");
    let trace_name_col = str_col!("trace_name");
    let start_time_col = i64_col!("start_time");
    let end_time_col = i64_col!("end_time");
    let duration_ms_col = i64_col!("duration_ms");
    let span_count_col = i64_col!("span_count");
    let service_col = str_col!("service_name");
    let has_error_col = i64_col!("has_error");

    let mut summaries = Vec::with_capacity(n);
    for i in 0..n {
        summaries.push(TraceSummary {
            trace_id: trace_id_col.value(i).to_string(),
            trace_name: trace_name_col.value(i).to_string(),
            start_time: start_time_col.value(i),
            end_time: end_time_col.value(i),
            duration_ms: duration_ms_col.value(i),
            span_count: span_count_col.value(i),
            service_name: service_col.value(i).to_string(),
            has_error: has_error_col.value(i) as i16,
        });
    }
    Ok(summaries)
}

/// Escape single quotes in a SQL string literal to prevent injection.
///
/// DataFusion is an in-process engine so there is no network attack surface,
/// but we still escape to avoid parse errors from user-provided strings.
fn escape_sql_str(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_sql_str() {
        assert_eq!(escape_sql_str("O'Brien"), "O''Brien");
        assert_eq!(escape_sql_str("normal"), "normal");
        assert_eq!(escape_sql_str(""), "");
    }

    #[test]
    fn test_pagination_defaults() {
        let p = Pagination::default();
        let (limit, offset) = pagination_values(&p);
        assert_eq!(limit, 50);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_pagination_custom() {
        let p = Pagination {
            limit: Some(100),
            offset: Some(200),
        };
        let (limit, offset) = pagination_values(&p);
        assert_eq!(limit, 100);
        assert_eq!(offset, 200);
    }

    #[test]
    fn test_trace_file_filter_converts_ns_to_us() {
        use zradar_traits::TimeRange;
        let filters = TraceQueryFilters {
            project_id: Some(Uuid::nil()),
            time_range: Some(TimeRange {
                start: 1_000_000_000,
                end: 2_000_000_000,
            }),
            ..Default::default()
        };
        let f = trace_file_filter(Uuid::nil(), &filters);
        assert_eq!(f.time_range_start, Some(1_000_000));
        assert_eq!(f.time_range_end, Some(2_000_000));
    }
}
