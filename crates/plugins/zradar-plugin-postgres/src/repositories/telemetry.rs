//! PostgreSQL telemetry repository with JSONB storage

use crate::client::PostgresClient;
use anyhow::Context;
use async_trait::async_trait;
use sqlx;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::{Metric, Span};
use zradar_traits::{
    PaginatedResponse, SpanQueryFilters, TelemetryReader, TelemetryWriter, TraceQueryFilters,
    TraceSummary,
};

pub struct PostgresTelemetryRepository {
    client: Arc<PostgresClient>,
}

impl PostgresTelemetryRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TelemetryWriter for PostgresTelemetryRepository {
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        for span in spans {
            // Convert strings to JSONB: parse as JSON if valid, otherwise wrap as JSON string
            let llm_input_jsonb: Option<serde_json::Value> = if span.llm_input.is_empty() {
                None
            } else {
                // Try parsing as JSON first, if that fails, treat as plain string and wrap in quotes
                match serde_json::from_str::<serde_json::Value>(&span.llm_input) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        // Not valid JSON, wrap as JSON string
                        Some(serde_json::Value::String(span.llm_input.clone()))
                    }
                }
            };

            let llm_output_jsonb: Option<serde_json::Value> = if span.llm_output.is_empty() {
                None
            } else {
                match serde_json::from_str::<serde_json::Value>(&span.llm_output) {
                    Ok(v) => Some(v),
                    Err(_) => Some(serde_json::Value::String(span.llm_output.clone())),
                }
            };

            let model_parameters_jsonb: Option<serde_json::Value> =
                if span.model_parameters.is_empty() {
                    None
                } else {
                    serde_json::from_str(&span.model_parameters).ok()
                };

            let attributes_jsonb: Option<serde_json::Value> = if span.attributes.is_empty() {
                None
            } else {
                serde_json::from_str(&span.attributes).ok()
            };

            sqlx::query(
                r#"
                INSERT INTO spans (
                    trace_id, span_id, parent_span_id, timestamp, duration_ns,
                    tenant_id, project_id, service_name, span_name, span_kind, span_type,
                    status_code, status_message, invocation_id, session_id, user_id,
                    agent_name, agent_type, llm_model, llm_input, llm_output,
                    prompt_tokens, completion_tokens, total_tokens,
                    prompt_cost_usd, completion_cost_usd, total_cost_usd,
                    tool_name, tool_call_id, resource_cpu_micros, resource_memory_bytes,
                    resource_memory_peak, prompt_id, prompt_name, prompt_version,
                    completion_start_time, time_to_first_token_ms, agent_version,
                    sdk_version, level, model_parameters, attributes,
                    created_at, updated_at, is_deleted
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
                    $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41,
                    $42, $43, $44, $45
                )
                ON CONFLICT (tenant_id, project_id, span_id) DO UPDATE SET
                    updated_at = $44,
                    attributes = EXCLUDED.attributes
                "#,
            )
            .bind(&span.trace_id)
            .bind(&span.span_id)
            .bind(&span.parent_span_id)
            .bind(span.timestamp)
            .bind(span.duration_ns as i64)
            .bind(&span.tenant_id)
            .bind(&span.project_id)
            .bind(&span.service_name)
            .bind(&span.span_name)
            .bind(&span.span_kind)
            .bind(&span.span_type)
            .bind(&span.status_code)
            .bind(&span.status_message)
            .bind(&span.invocation_id)
            .bind(&span.session_id)
            .bind(&span.user_id)
            .bind(&span.agent_name)
            .bind(&span.agent_type)
            .bind(&span.llm_model)
            .bind(&llm_input_jsonb)
            .bind(&llm_output_jsonb)
            .bind(span.prompt_tokens as i32)
            .bind(span.completion_tokens as i32)
            .bind(span.total_tokens as i32)
            .bind(span.prompt_cost_usd)
            .bind(span.completion_cost_usd)
            .bind(span.total_cost_usd)
            .bind(&span.tool_name)
            .bind(&span.tool_call_id)
            .bind(span.resource_cpu_micros as i64)
            .bind(span.resource_memory_bytes as i64)
            .bind(span.resource_memory_peak as i64)
            .bind(&span.prompt_id)
            .bind(&span.prompt_name)
            .bind(span.prompt_version as i32)
            .bind(span.completion_start_time)
            .bind(span.time_to_first_token_ms as i32)
            .bind(&span.agent_version)
            .bind(&span.sdk_version)
            .bind(&span.level)
            .bind(&model_parameters_jsonb)
            .bind(&attributes_jsonb)
            .bind(span.created_at)
            .bind(span.updated_at)
            .bind(span.is_deleted as i16)
            .execute(self.client.pool())
            .await
            .context("Failed to insert span")?;
        }

        tracing::debug!("Inserted {} spans (with JSONB storage)", spans.len());
        Ok(())
    }

    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        for metric in metrics {
            sqlx::query(
                r#"
                INSERT INTO metrics (
                    metric_name, metric_type, timestamp, tenant_id, project_id,
                    value, count, sum, min, max,
                    service_name, agent_name, user_id, session_id, labels
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
                )
                "#,
            )
            .bind(&metric.metric_name)
            .bind(&metric.metric_type)
            .bind(metric.timestamp)
            .bind(&metric.tenant_id)
            .bind(&metric.project_id)
            .bind(metric.value)
            .bind(metric.count as i64)
            .bind(metric.sum)
            .bind(metric.min)
            .bind(metric.max)
            .bind(&metric.service_name)
            .bind(&metric.agent_name)
            .bind(&metric.user_id)
            .bind(&metric.session_id)
            .bind(&metric.labels)
            .execute(self.client.pool())
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl TelemetryReader for PostgresTelemetryRepository {
    async fn query_traces(
        &self,
        filters: TraceQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<TraceSummary>> {
        let project_id = filters
            .project_id
            .map(|p| p.to_string())
            .unwrap_or_default();
        let limit = filters.pagination.limit.unwrap_or(100);
        let offset = filters.pagination.offset.unwrap_or(0);

        // Build dynamic query based on filters
        let mut query = String::from(
            r#"
            SELECT 
                trace_id,
                MIN(span_name) as trace_name,
                MIN(timestamp) as start_time,
                MAX(timestamp + duration_ns) as end_time,
                (MAX(timestamp + duration_ns) - MIN(timestamp)) / 1000000 as duration_ms,
                COUNT(*)::BIGINT as span_count,
                MIN(service_name) as service_name,
                MAX(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::SMALLINT as has_error
            FROM spans
            WHERE project_id = $1 AND is_deleted = 0
            "#,
        );

        let mut param_count = 1;

        if filters.service_name.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND service_name = ${}", param_count));
        }

        if let Some(_time_range) = &filters.time_range {
            param_count += 1;
            query.push_str(&format!(" AND timestamp >= ${}", param_count));
            param_count += 1;
            query.push_str(&format!(" AND timestamp <= ${}", param_count));
        }

        query.push_str(" GROUP BY trace_id ORDER BY start_time DESC");
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        let mut q = sqlx::query_as::<_, TraceSummary>(&query).bind(&project_id);

        if let Some(service) = &filters.service_name {
            q = q.bind(service);
        }
        if let Some(ref time_range) = filters.time_range {
            q = q.bind(time_range.start);
            q = q.bind(time_range.end);
        }

        let traces = q.fetch_all(self.client.pool()).await?;

        Ok(PaginatedResponse {
            total: traces.len() as u64,
            items: traces,
            limit,
            offset,
        })
    }

    async fn get_trace_detail(
        &self,
        project_id: Uuid,
        trace_id: &str,
    ) -> anyhow::Result<Option<Vec<Span>>> {
        // Custom struct to handle JSONB fields
        #[derive(sqlx::FromRow)]
        struct SpanRow {
            trace_id: String,
            span_id: String,
            parent_span_id: String,
            timestamp: i64,
            duration_ns: i64,
            tenant_id: String,
            project_id: String,
            service_name: String,
            span_name: String,
            span_kind: String,
            span_type: String,
            status_code: String,
            status_message: String,
            invocation_id: String,
            session_id: String,
            user_id: String,
            agent_name: String,
            agent_type: String,
            llm_model: String,
            #[sqlx(rename = "llm_input")]
            llm_input_text: Option<String>,
            #[sqlx(rename = "llm_output")]
            llm_output_text: Option<String>,
            prompt_tokens: i32,
            completion_tokens: i32,
            total_tokens: i32,
            prompt_cost_usd: f64,
            completion_cost_usd: f64,
            total_cost_usd: f64,
            tool_name: String,
            tool_call_id: String,
            resource_cpu_micros: i64,
            resource_memory_bytes: i64,
            resource_memory_peak: i64,
            prompt_id: String,
            prompt_name: String,
            prompt_version: i32,
            completion_start_time: Option<i64>,
            time_to_first_token_ms: i32,
            agent_version: String,
            sdk_version: String,
            level: String,
            #[sqlx(rename = "model_parameters")]
            model_parameters_text: Option<String>,
            #[sqlx(rename = "attributes")]
            attributes_text: Option<String>,
            created_at: i64,
            updated_at: i64,
            is_deleted: i16,
        }

        let rows = sqlx::query_as::<_, SpanRow>(
            r#"
            SELECT 
                trace_id, span_id, parent_span_id, timestamp, duration_ns,
                tenant_id, project_id, service_name, span_name, span_kind, span_type,
                status_code, status_message, invocation_id, session_id, user_id,
                agent_name, agent_type, llm_model,
                llm_input::text as llm_input,
                llm_output::text as llm_output,
                prompt_tokens, completion_tokens, total_tokens,
                prompt_cost_usd, completion_cost_usd, total_cost_usd,
                tool_name, tool_call_id, resource_cpu_micros, resource_memory_bytes,
                resource_memory_peak, prompt_id, prompt_name, prompt_version,
                completion_start_time, time_to_first_token_ms, agent_version,
                sdk_version, level,
                model_parameters::text as model_parameters,
                attributes::text as attributes,
                created_at, updated_at, is_deleted
            FROM spans
            WHERE project_id = $1 AND trace_id = $2 AND is_deleted = 0
            ORDER BY timestamp ASC
            "#,
        )
        .bind(project_id.to_string())
        .bind(trace_id)
        .fetch_all(self.client.pool())
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        // Convert to Span (JSONB fields are already strings)
        let spans: Vec<Span> = rows
            .into_iter()
            .map(|row| Span {
                trace_id: row.trace_id,
                span_id: row.span_id,
                parent_span_id: row.parent_span_id,
                timestamp: row.timestamp,
                duration_ns: row.duration_ns,
                tenant_id: row.tenant_id,
                project_id: row.project_id,
                service_name: row.service_name,
                span_name: row.span_name,
                span_kind: row.span_kind,
                span_type: row.span_type,
                status_code: row.status_code,
                status_message: row.status_message,
                invocation_id: row.invocation_id,
                session_id: row.session_id,
                user_id: row.user_id,
                agent_name: row.agent_name,
                agent_type: row.agent_type,
                llm_model: row.llm_model,
                llm_input: row.llm_input_text.unwrap_or_default(),
                llm_output: row.llm_output_text.unwrap_or_default(),
                prompt_tokens: row.prompt_tokens,
                completion_tokens: row.completion_tokens,
                total_tokens: row.total_tokens,
                prompt_cost_usd: row.prompt_cost_usd,
                completion_cost_usd: row.completion_cost_usd,
                total_cost_usd: row.total_cost_usd,
                tool_name: row.tool_name,
                tool_call_id: row.tool_call_id,
                resource_cpu_micros: row.resource_cpu_micros,
                resource_memory_bytes: row.resource_memory_bytes,
                resource_memory_peak: row.resource_memory_peak,
                prompt_id: row.prompt_id,
                prompt_name: row.prompt_name,
                prompt_version: row.prompt_version,
                completion_start_time: row.completion_start_time,
                time_to_first_token_ms: row.time_to_first_token_ms,
                agent_version: row.agent_version,
                sdk_version: row.sdk_version,
                level: row.level,
                model_parameters: row
                    .model_parameters_text
                    .unwrap_or_else(|| "{}".to_string()),
                attributes: row.attributes_text.unwrap_or_else(|| "{}".to_string()),
                created_at: row.created_at,
                updated_at: row.updated_at,
                is_deleted: row.is_deleted,
            })
            .collect();

        Ok(Some(spans))
    }

    async fn query_spans(
        &self,
        filters: SpanQueryFilters,
    ) -> anyhow::Result<PaginatedResponse<Span>> {
        let project_id = filters
            .project_id
            .map(|p| p.to_string())
            .unwrap_or_default();
        let limit = filters.pagination.limit.unwrap_or(100);
        let offset = filters.pagination.offset.unwrap_or(0);

        // Build SELECT clause with explicit columns (JSONB cast to text)
        let select_clause = r#"
            trace_id, span_id, parent_span_id, timestamp, duration_ns,
            tenant_id, project_id, service_name, span_name, span_kind, span_type,
            status_code, status_message, invocation_id, session_id, user_id,
            agent_name, agent_type, llm_model,
            llm_input::text as llm_input,
            llm_output::text as llm_output,
            prompt_tokens, completion_tokens, total_tokens,
            prompt_cost_usd, completion_cost_usd, total_cost_usd,
            tool_name, tool_call_id, resource_cpu_micros, resource_memory_bytes,
            resource_memory_peak, prompt_id, prompt_name, prompt_version,
            completion_start_time, time_to_first_token_ms, agent_version,
            sdk_version, level,
            model_parameters::text as model_parameters,
            attributes::text as attributes,
            created_at, updated_at, is_deleted
        "#;

        let mut query = format!(
            "SELECT {} FROM spans WHERE project_id = $1 AND is_deleted = 0",
            select_clause
        );

        let mut param_count = 1;

        if filters.trace_id.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND trace_id = ${}", param_count));
        }

        if filters.span_name.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND span_name ILIKE ${}", param_count));
        }

        if let Some(ref span_types) = filters.span_types {
            if !span_types.is_empty() {
                if span_types.len() == 1 {
                    param_count += 1;
                    query.push_str(&format!(" AND span_type = ${}", param_count));
                } else {
                    param_count += 1;
                    query.push_str(&format!(" AND span_type = ANY(${})", param_count));
                }
            }
        }

        query.push_str(" ORDER BY timestamp DESC");
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        // Use SpanRow struct to handle JSONB fields
        #[derive(sqlx::FromRow)]
        struct SpanRow {
            trace_id: String,
            span_id: String,
            parent_span_id: String,
            timestamp: i64,
            duration_ns: i64,
            tenant_id: String,
            project_id: String,
            service_name: String,
            span_name: String,
            span_kind: String,
            span_type: String,
            status_code: String,
            status_message: String,
            invocation_id: String,
            session_id: String,
            user_id: String,
            agent_name: String,
            agent_type: String,
            llm_model: String,
            #[sqlx(rename = "llm_input")]
            llm_input_text: Option<String>,
            #[sqlx(rename = "llm_output")]
            llm_output_text: Option<String>,
            prompt_tokens: i32,
            completion_tokens: i32,
            total_tokens: i32,
            prompt_cost_usd: f64,
            completion_cost_usd: f64,
            total_cost_usd: f64,
            tool_name: String,
            tool_call_id: String,
            resource_cpu_micros: i64,
            resource_memory_bytes: i64,
            resource_memory_peak: i64,
            prompt_id: String,
            prompt_name: String,
            prompt_version: i32,
            completion_start_time: Option<i64>,
            time_to_first_token_ms: i32,
            agent_version: String,
            sdk_version: String,
            level: String,
            #[sqlx(rename = "model_parameters")]
            model_parameters_text: Option<String>,
            #[sqlx(rename = "attributes")]
            attributes_text: Option<String>,
            created_at: i64,
            updated_at: i64,
            is_deleted: i16,
        }

        let mut q = sqlx::query_as::<_, SpanRow>(&query).bind(&project_id);

        if let Some(trace_id) = &filters.trace_id {
            q = q.bind(trace_id);
        }
        if let Some(name) = &filters.span_name {
            q = q.bind(format!("%{}%", name));
        }
        if let Some(ref span_types) = filters.span_types {
            if !span_types.is_empty() {
                if span_types.len() == 1 {
                    q = q.bind(&span_types[0]);
                } else {
                    q = q.bind(span_types);
                }
            }
        }

        let rows = q.fetch_all(self.client.pool()).await?;

        // Convert to Span (JSONB fields are already strings)
        let items: Vec<Span> = rows
            .into_iter()
            .map(|row| Span {
                trace_id: row.trace_id,
                span_id: row.span_id,
                parent_span_id: row.parent_span_id,
                timestamp: row.timestamp,
                duration_ns: row.duration_ns,
                tenant_id: row.tenant_id,
                project_id: row.project_id,
                service_name: row.service_name,
                span_name: row.span_name,
                span_kind: row.span_kind,
                span_type: row.span_type,
                status_code: row.status_code,
                status_message: row.status_message,
                invocation_id: row.invocation_id,
                session_id: row.session_id,
                user_id: row.user_id,
                agent_name: row.agent_name,
                agent_type: row.agent_type,
                llm_model: row.llm_model,
                llm_input: row.llm_input_text.unwrap_or_default(),
                llm_output: row.llm_output_text.unwrap_or_default(),
                prompt_tokens: row.prompt_tokens,
                completion_tokens: row.completion_tokens,
                total_tokens: row.total_tokens,
                prompt_cost_usd: row.prompt_cost_usd,
                completion_cost_usd: row.completion_cost_usd,
                total_cost_usd: row.total_cost_usd,
                tool_name: row.tool_name,
                tool_call_id: row.tool_call_id,
                resource_cpu_micros: row.resource_cpu_micros,
                resource_memory_bytes: row.resource_memory_bytes,
                resource_memory_peak: row.resource_memory_peak,
                prompt_id: row.prompt_id,
                prompt_name: row.prompt_name,
                prompt_version: row.prompt_version,
                completion_start_time: row.completion_start_time,
                time_to_first_token_ms: row.time_to_first_token_ms,
                agent_version: row.agent_version,
                sdk_version: row.sdk_version,
                level: row.level,
                model_parameters: row
                    .model_parameters_text
                    .unwrap_or_else(|| "{}".to_string()),
                attributes: row.attributes_text.unwrap_or_else(|| "{}".to_string()),
                created_at: row.created_at,
                updated_at: row.updated_at,
                is_deleted: row.is_deleted,
            })
            .collect();
        let total = items.len() as u64;

        Ok(PaginatedResponse {
            total,
            items,
            limit,
            offset,
        })
    }

    async fn get_span(&self, project_id: Uuid, span_id: &str) -> anyhow::Result<Option<Span>> {
        // Use SpanRow struct to handle JSONB fields
        #[derive(sqlx::FromRow)]
        struct SpanRow {
            trace_id: String,
            span_id: String,
            parent_span_id: String,
            timestamp: i64,
            duration_ns: i64,
            tenant_id: String,
            project_id: String,
            service_name: String,
            span_name: String,
            span_kind: String,
            span_type: String,
            status_code: String,
            status_message: String,
            invocation_id: String,
            session_id: String,
            user_id: String,
            agent_name: String,
            agent_type: String,
            llm_model: String,
            #[sqlx(rename = "llm_input")]
            llm_input_text: Option<String>,
            #[sqlx(rename = "llm_output")]
            llm_output_text: Option<String>,
            prompt_tokens: i32,
            completion_tokens: i32,
            total_tokens: i32,
            prompt_cost_usd: f64,
            completion_cost_usd: f64,
            total_cost_usd: f64,
            tool_name: String,
            tool_call_id: String,
            resource_cpu_micros: i64,
            resource_memory_bytes: i64,
            resource_memory_peak: i64,
            prompt_id: String,
            prompt_name: String,
            prompt_version: i32,
            completion_start_time: Option<i64>,
            time_to_first_token_ms: i32,
            agent_version: String,
            sdk_version: String,
            level: String,
            #[sqlx(rename = "model_parameters")]
            model_parameters_text: Option<String>,
            #[sqlx(rename = "attributes")]
            attributes_text: Option<String>,
            created_at: i64,
            updated_at: i64,
            is_deleted: i16,
        }

        let row = sqlx::query_as::<_, SpanRow>(
            r#"
            SELECT 
                trace_id, span_id, parent_span_id, timestamp, duration_ns,
                tenant_id, project_id, service_name, span_name, span_kind, span_type,
                status_code, status_message, invocation_id, session_id, user_id,
                agent_name, agent_type, llm_model,
                llm_input::text as llm_input,
                llm_output::text as llm_output,
                prompt_tokens, completion_tokens, total_tokens,
                prompt_cost_usd, completion_cost_usd, total_cost_usd,
                tool_name, tool_call_id, resource_cpu_micros, resource_memory_bytes,
                resource_memory_peak, prompt_id, prompt_name, prompt_version,
                completion_start_time, time_to_first_token_ms, agent_version,
                sdk_version, level,
                model_parameters::text as model_parameters,
                attributes::text as attributes,
                created_at, updated_at, is_deleted
            FROM spans
            WHERE project_id = $1 AND span_id = $2 AND is_deleted = 0
            "#,
        )
        .bind(project_id.to_string())
        .bind(span_id)
        .fetch_optional(self.client.pool())
        .await?;

        if let Some(row) = row {
            Ok(Some(Span {
                trace_id: row.trace_id,
                span_id: row.span_id,
                parent_span_id: row.parent_span_id,
                timestamp: row.timestamp,
                duration_ns: row.duration_ns,
                tenant_id: row.tenant_id,
                project_id: row.project_id,
                service_name: row.service_name,
                span_name: row.span_name,
                span_kind: row.span_kind,
                span_type: row.span_type,
                status_code: row.status_code,
                status_message: row.status_message,
                invocation_id: row.invocation_id,
                session_id: row.session_id,
                user_id: row.user_id,
                agent_name: row.agent_name,
                agent_type: row.agent_type,
                llm_model: row.llm_model,
                llm_input: row.llm_input_text.unwrap_or_default(),
                llm_output: row.llm_output_text.unwrap_or_default(),
                prompt_tokens: row.prompt_tokens,
                completion_tokens: row.completion_tokens,
                total_tokens: row.total_tokens,
                prompt_cost_usd: row.prompt_cost_usd,
                completion_cost_usd: row.completion_cost_usd,
                total_cost_usd: row.total_cost_usd,
                tool_name: row.tool_name,
                tool_call_id: row.tool_call_id,
                resource_cpu_micros: row.resource_cpu_micros,
                resource_memory_bytes: row.resource_memory_bytes,
                resource_memory_peak: row.resource_memory_peak,
                prompt_id: row.prompt_id,
                prompt_name: row.prompt_name,
                prompt_version: row.prompt_version,
                completion_start_time: row.completion_start_time,
                time_to_first_token_ms: row.time_to_first_token_ms,
                agent_version: row.agent_version,
                sdk_version: row.sdk_version,
                level: row.level,
                model_parameters: row
                    .model_parameters_text
                    .unwrap_or_else(|| "{}".to_string()),
                attributes: row.attributes_text.unwrap_or_else(|| "{}".to_string()),
                created_at: row.created_at,
                updated_at: row.updated_at,
                is_deleted: row.is_deleted,
            }))
        } else {
            Ok(None)
        }
    }
}

use zradar_traits::repositories::telemetry::{AnalyticsReader, MetricsSummary, TimeSeriesPoint};

#[async_trait]
impl AnalyticsReader for PostgresTelemetryRepository {
    async fn get_daily_trace_counts(
        &self,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<TimeSeriesPoint>> {
        // Postgres implementation for daily trace counts
        let query = r#"
            SELECT 
                to_char(to_timestamp(timestamp / 1000000000)::date, 'YYYY-MM-DD"T"00:00:00"Z"') as timestamp,
                COUNT(*)::FLOAT8 as value
            FROM spans
            WHERE project_id = $1 
              AND timestamp >= $2 
              AND timestamp <= $3
              AND parent_span_id = '' -- Only count root spans
            GROUP BY to_timestamp(timestamp / 1000000000)::date
            ORDER BY timestamp ASC
        "#;

        #[derive(sqlx::FromRow)]
        struct PointRow {
            timestamp: String,
            value: f64,
        }

        let rows = sqlx::query_as::<_, PointRow>(query)
            .bind(project_id.to_string())
            .bind(start)
            .bind(end)
            .fetch_all(self.client.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|r| TimeSeriesPoint {
                timestamp: r.timestamp,
                value: r.value,
            })
            .collect())
    }

    async fn get_metrics_summary(
        &self,
        project_id: Uuid,
        start: i64,
        end: i64,
    ) -> anyhow::Result<MetricsSummary> {
        // Postgres implementation for metrics summary
        // Note: Percentile calculations in Postgres are more complex/slow without extensions like t-digest,
        // so we'll use simple approximations or just AVG for now for dev purposes if percentiles are hard.
        // Actually, let's use percentile_cont.
        let query = r#"
            SELECT 
                COUNT(*)::BIGINT as total_traces,
                (COUNT(*) FILTER (WHERE status_code = 'ERROR'))::FLOAT8 / NULLIF(COUNT(*), 0) as error_rate,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ns)::FLOAT8 / 1000000 as p50_latency,
                percentile_cont(0.9) WITHIN GROUP (ORDER BY duration_ns)::FLOAT8 / 1000000 as p90_latency,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ns)::FLOAT8 / 1000000 as p99_latency
            FROM spans
            WHERE project_id = $1
              AND timestamp >= $2
              AND timestamp <= $3
              AND parent_span_id = ''
        "#;

        #[derive(sqlx::FromRow)]
        struct SummaryRow {
            total_traces: i64,
            error_rate: Option<f64>,
            p50_latency: Option<f64>,
            p90_latency: Option<f64>,
            p99_latency: Option<f64>,
        }

        let row = sqlx::query_as::<_, SummaryRow>(query)
            .bind(project_id.to_string())
            .bind(start)
            .bind(end)
            .fetch_optional(self.client.pool())
            .await?;

        if let Some(r) = row {
            Ok(MetricsSummary {
                total_traces: r.total_traces,
                error_rate: r.error_rate.unwrap_or(0.0),
                p50_latency: r.p50_latency.unwrap_or(0.0),
                p90_latency: r.p90_latency.unwrap_or(0.0),
                p99_latency: r.p99_latency.unwrap_or(0.0),
            })
        } else {
            Ok(MetricsSummary::default())
        }
    }
}
