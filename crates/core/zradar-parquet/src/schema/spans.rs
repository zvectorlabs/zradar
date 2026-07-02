//! Arrow schema and RecordBatch ↔ Span conversions.
//!
//! Maps every field in `zradar_models::Span` to an Arrow column type.
//! `spans_to_record_batch` converts row-oriented Span structs to columnar Arrow format.
//! `record_batch_to_spans` is the inverse, used by the read path.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zradar_models::Span;

/// Return the Arrow schema for the Span type.
///
/// Column order matches the field order in `Span` to make RecordBatch
/// construction straightforward.
pub fn span_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, false),
        // Timing (nanoseconds)
        Field::new("timestamp", DataType::Int64, false),
        Field::new("duration_ns", DataType::Int64, false),
        // Multi-tenancy
        Field::new("workspace_id", DataType::Utf8, false),
        // Service metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Utf8, false),
        Field::new("span_type", DataType::Utf8, false),
        // Status
        Field::new("status_code", DataType::Utf8, false),
        Field::new("status_message", DataType::Utf8, false),
        // Agent context
        Field::new("invocation_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("agent_name", DataType::Utf8, false),
        Field::new("agent_type", DataType::Utf8, false),
        // LLM text fields
        Field::new("llm_model", DataType::Utf8, false),
        Field::new("llm_input", DataType::Utf8, false),
        Field::new("llm_output", DataType::Utf8, false),
        // Token counts
        Field::new("prompt_tokens", DataType::Int32, false),
        Field::new("completion_tokens", DataType::Int32, false),
        Field::new("total_tokens", DataType::Int32, false),
        // Cost tracking
        Field::new("prompt_cost_usd", DataType::Float64, false),
        Field::new("completion_cost_usd", DataType::Float64, false),
        Field::new("total_cost_usd", DataType::Float64, false),
        // Tool
        Field::new("tool_name", DataType::Utf8, false),
        Field::new("tool_call_id", DataType::Utf8, false),
        // Guardrails (Phase 0 R0.2 – R0.4)
        Field::new("rail_type", DataType::Utf8, false),
        Field::new("rail_name", DataType::Utf8, false),
        Field::new("rail_stop", DataType::Int16, false),
        Field::new("action_name", DataType::Utf8, false),
        Field::new("action_has_llm_calls", DataType::Int16, false),
        Field::new("action_llm_calls_count", DataType::Int32, false),
        // NeMo Agent Toolkit + OTel GenAI 1.29 (Phase 1 R1.2 – R1.6)
        Field::new("workflow_run_id", DataType::Utf8, false),
        Field::new("framework", DataType::Utf8, false),
        Field::new("llm_provider", DataType::Utf8, false),
        Field::new("llm_response_model", DataType::Utf8, false),
        Field::new("events", DataType::Utf8, false),
        // Phase 4 polish (R4.2 – R4.6)
        Field::new("llm_cache_hit", DataType::Int16, false),
        Field::new("llm_response_id", DataType::Utf8, false),
        Field::new("environment", DataType::Utf8, false),
        Field::new("links", DataType::Utf8, false),
        // Resource profiling
        Field::new("resource_cpu_micros", DataType::Int64, false),
        Field::new("resource_memory_bytes", DataType::Int64, false),
        Field::new("resource_memory_peak", DataType::Int64, false),
        // Prompt management
        Field::new("prompt_id", DataType::Utf8, false),
        Field::new("prompt_name", DataType::Utf8, false),
        Field::new("prompt_version", DataType::Int32, false),
        // Timing details — completion_start_time is nullable
        Field::new("completion_start_time", DataType::Int64, true),
        Field::new("time_to_first_token_ms", DataType::Int32, false),
        // Versioning
        Field::new("agent_version", DataType::Utf8, false),
        Field::new("sdk_version", DataType::Utf8, false),
        // Severity
        Field::new("level", DataType::Utf8, false),
        // Database
        Field::new("db_system_name", DataType::Utf8, false),
        Field::new("db_namespace", DataType::Utf8, false),
        Field::new("db_operation_name", DataType::Utf8, false),
        Field::new("db_query_text", DataType::Utf8, false),
        Field::new("db_query_summary", DataType::Utf8, false),
        Field::new("db_collection_name", DataType::Utf8, false),
        Field::new("db_response_status_code", DataType::Utf8, false),
        // Flexible attributes (stored as JSON strings)
        Field::new("model_parameters", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        // Lifecycle
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("is_deleted", DataType::Int16, false),
    ])
}

/// Convert a slice of `Span` rows into an Arrow `RecordBatch`.
///
/// Builds one columnar array per field and bundles them into a RecordBatch
/// using the schema returned by [`span_arrow_schema`].
pub fn spans_to_record_batch(spans: &[Span]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(span_arrow_schema());

    // Identity
    let trace_id = StringArray::from_iter_values(spans.iter().map(|s| s.trace_id.as_str()));
    let span_id = StringArray::from_iter_values(spans.iter().map(|s| s.span_id.as_str()));
    let parent_span_id =
        StringArray::from_iter_values(spans.iter().map(|s| s.parent_span_id.as_str()));
    // Timing
    let timestamp: Int64Array = spans.iter().map(|s| s.timestamp).collect();
    let duration_ns: Int64Array = spans.iter().map(|s| s.duration_ns).collect();
    // Multi-tenancy
    let workspace_id = StringArray::from_iter_values(spans.iter().map(|s| s.workspace_id.as_str()));
    // Service metadata
    let service_name = StringArray::from_iter_values(spans.iter().map(|s| s.service_name.as_str()));
    let span_name = StringArray::from_iter_values(spans.iter().map(|s| s.span_name.as_str()));
    let span_kind = StringArray::from_iter_values(spans.iter().map(|s| s.span_kind.as_str()));
    let span_type = StringArray::from_iter_values(spans.iter().map(|s| s.span_type.as_str()));
    // Status
    let status_code = StringArray::from_iter_values(spans.iter().map(|s| s.status_code.as_str()));
    let status_message =
        StringArray::from_iter_values(spans.iter().map(|s| s.status_message.as_str()));
    // Agent context
    let invocation_id =
        StringArray::from_iter_values(spans.iter().map(|s| s.invocation_id.as_str()));
    let session_id = StringArray::from_iter_values(spans.iter().map(|s| s.session_id.as_str()));
    let user_id = StringArray::from_iter_values(spans.iter().map(|s| s.user_id.as_str()));
    let agent_name = StringArray::from_iter_values(spans.iter().map(|s| s.agent_name.as_str()));
    let agent_type = StringArray::from_iter_values(spans.iter().map(|s| s.agent_type.as_str()));
    // LLM
    let llm_model = StringArray::from_iter_values(spans.iter().map(|s| s.llm_model.as_str()));
    let llm_input = StringArray::from_iter_values(spans.iter().map(|s| s.llm_input.as_str()));
    let llm_output = StringArray::from_iter_values(spans.iter().map(|s| s.llm_output.as_str()));
    // Token counts
    let prompt_tokens: Int32Array = spans.iter().map(|s| s.prompt_tokens).collect();
    let completion_tokens: Int32Array = spans.iter().map(|s| s.completion_tokens).collect();
    let total_tokens: Int32Array = spans.iter().map(|s| s.total_tokens).collect();
    // Costs
    let prompt_cost_usd: Float64Array = spans.iter().map(|s| s.prompt_cost_usd).collect();
    let completion_cost_usd: Float64Array = spans.iter().map(|s| s.completion_cost_usd).collect();
    let total_cost_usd: Float64Array = spans.iter().map(|s| s.total_cost_usd).collect();
    // Tool
    let tool_name = StringArray::from_iter_values(spans.iter().map(|s| s.tool_name.as_str()));
    let tool_call_id = StringArray::from_iter_values(spans.iter().map(|s| s.tool_call_id.as_str()));
    // Guardrails (Phase 0)
    let rail_type = StringArray::from_iter_values(spans.iter().map(|s| s.rail_type.as_str()));
    let rail_name = StringArray::from_iter_values(spans.iter().map(|s| s.rail_name.as_str()));
    let rail_stop: Int16Array = spans.iter().map(|s| s.rail_stop).collect();
    let action_name = StringArray::from_iter_values(spans.iter().map(|s| s.action_name.as_str()));
    let action_has_llm_calls: Int16Array = spans.iter().map(|s| s.action_has_llm_calls).collect();
    let action_llm_calls_count: Int32Array =
        spans.iter().map(|s| s.action_llm_calls_count).collect();
    // NeMo Phase 1
    let workflow_run_id =
        StringArray::from_iter_values(spans.iter().map(|s| s.workflow_run_id.as_str()));
    let framework = StringArray::from_iter_values(spans.iter().map(|s| s.framework.as_str()));
    let llm_provider = StringArray::from_iter_values(spans.iter().map(|s| s.llm_provider.as_str()));
    let llm_response_model =
        StringArray::from_iter_values(spans.iter().map(|s| s.llm_response_model.as_str()));
    let events = StringArray::from_iter_values(spans.iter().map(|s| s.events.as_str()));
    // Phase 4
    let llm_cache_hit: Int16Array = spans.iter().map(|s| s.llm_cache_hit).collect();
    let llm_response_id =
        StringArray::from_iter_values(spans.iter().map(|s| s.llm_response_id.as_str()));
    let environment = StringArray::from_iter_values(spans.iter().map(|s| s.environment.as_str()));
    let links = StringArray::from_iter_values(spans.iter().map(|s| s.links.as_str()));
    // Resource
    let resource_cpu_micros: Int64Array = spans.iter().map(|s| s.resource_cpu_micros).collect();
    let resource_memory_bytes: Int64Array = spans.iter().map(|s| s.resource_memory_bytes).collect();
    let resource_memory_peak: Int64Array = spans.iter().map(|s| s.resource_memory_peak).collect();
    // Prompt management
    let prompt_id = StringArray::from_iter_values(spans.iter().map(|s| s.prompt_id.as_str()));
    let prompt_name = StringArray::from_iter_values(spans.iter().map(|s| s.prompt_name.as_str()));
    let prompt_version: Int32Array = spans.iter().map(|s| s.prompt_version).collect();
    // Timing details — completion_start_time is nullable Option<i64>
    let completion_start_time: Int64Array = spans.iter().map(|s| s.completion_start_time).collect();
    let time_to_first_token_ms: Int32Array =
        spans.iter().map(|s| s.time_to_first_token_ms).collect();
    // Versioning
    let agent_version =
        StringArray::from_iter_values(spans.iter().map(|s| s.agent_version.as_str()));
    let sdk_version = StringArray::from_iter_values(spans.iter().map(|s| s.sdk_version.as_str()));
    // Severity
    let level = StringArray::from_iter_values(spans.iter().map(|s| s.level.as_str()));
    // Database
    let db_system_name =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_system_name.as_str()));
    let db_namespace = StringArray::from_iter_values(spans.iter().map(|s| s.db_namespace.as_str()));
    let db_operation_name =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_operation_name.as_str()));
    let db_query_text =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_query_text.as_str()));
    let db_query_summary =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_query_summary.as_str()));
    let db_collection_name =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_collection_name.as_str()));
    let db_response_status_code =
        StringArray::from_iter_values(spans.iter().map(|s| s.db_response_status_code.as_str()));
    // Flexible attributes
    let model_parameters =
        StringArray::from_iter_values(spans.iter().map(|s| s.model_parameters.as_str()));
    let attributes = StringArray::from_iter_values(spans.iter().map(|s| s.attributes.as_str()));
    // Lifecycle
    let created_at: Int64Array = spans.iter().map(|s| s.created_at).collect();
    let updated_at: Int64Array = spans.iter().map(|s| s.updated_at).collect();
    let is_deleted: Int16Array = spans.iter().map(|s| s.is_deleted).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace_id),
        Arc::new(span_id),
        Arc::new(parent_span_id),
        Arc::new(timestamp),
        Arc::new(duration_ns),
        Arc::new(workspace_id),
        Arc::new(service_name),
        Arc::new(span_name),
        Arc::new(span_kind),
        Arc::new(span_type),
        Arc::new(status_code),
        Arc::new(status_message),
        Arc::new(invocation_id),
        Arc::new(session_id),
        Arc::new(user_id),
        Arc::new(agent_name),
        Arc::new(agent_type),
        Arc::new(llm_model),
        Arc::new(llm_input),
        Arc::new(llm_output),
        Arc::new(prompt_tokens),
        Arc::new(completion_tokens),
        Arc::new(total_tokens),
        Arc::new(prompt_cost_usd),
        Arc::new(completion_cost_usd),
        Arc::new(total_cost_usd),
        Arc::new(tool_name),
        Arc::new(tool_call_id),
        Arc::new(rail_type),
        Arc::new(rail_name),
        Arc::new(rail_stop),
        Arc::new(action_name),
        Arc::new(action_has_llm_calls),
        Arc::new(action_llm_calls_count),
        Arc::new(workflow_run_id),
        Arc::new(framework),
        Arc::new(llm_provider),
        Arc::new(llm_response_model),
        Arc::new(events),
        Arc::new(llm_cache_hit),
        Arc::new(llm_response_id),
        Arc::new(environment),
        Arc::new(links),
        Arc::new(resource_cpu_micros),
        Arc::new(resource_memory_bytes),
        Arc::new(resource_memory_peak),
        Arc::new(prompt_id),
        Arc::new(prompt_name),
        Arc::new(prompt_version),
        Arc::new(completion_start_time),
        Arc::new(time_to_first_token_ms),
        Arc::new(agent_version),
        Arc::new(sdk_version),
        Arc::new(level),
        Arc::new(db_system_name),
        Arc::new(db_namespace),
        Arc::new(db_operation_name),
        Arc::new(db_query_text),
        Arc::new(db_query_summary),
        Arc::new(db_collection_name),
        Arc::new(db_response_status_code),
        Arc::new(model_parameters),
        Arc::new(attributes),
        Arc::new(created_at),
        Arc::new(updated_at),
        Arc::new(is_deleted),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to construct Span RecordBatch")
}

/// Look up an optional `StringArray` column by name.
///
/// Returns `Ok(Some(arr))` if the column exists and is a `StringArray`,
/// `Ok(None)` if the column is absent from the batch (caller should default),
/// and `Err` if the column exists but has the wrong Arrow type (a real schema
/// mismatch that should not be silently defaulted).
///
/// Foundation for Phase 0/1 of the NeMo compatibility work
/// (`zradar-plans/nemo-compatibility/techspec/TECH-SPEC-PHASE-0.md` §4.5).
/// Used by [`record_batch_to_spans`] so that older Parquet files written
/// before a column existed can still be read after the `Span` struct grows.
pub fn optional_string_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> anyhow::Result<Option<&'a StringArray>> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(Some)
            .ok_or_else(|| anyhow!("column {} is not StringArray", name)),
    }
}

/// Look up an optional `Int16Array` column by name.
///
/// Mirrors [`optional_string_col`] for `Int16Array` columns. Returns
/// `Ok(None)` when the column is absent so the reader can default the
/// field; returns `Err` if it exists with the wrong Arrow type.
///
/// Used so that older Parquet files written before a `bool-as-i16` column
/// existed (see Phase 0 PR4 / NeMo Guardrails) still read cleanly.
pub fn optional_i16_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> anyhow::Result<Option<&'a Int16Array>> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(Some)
            .ok_or_else(|| anyhow!("column {} is not Int16Array", name)),
    }
}

/// Look up an optional `Int32Array` column by name.
///
/// Mirrors [`optional_string_col`] for `Int32Array` columns. Returns
/// `Ok(None)` when the column is absent so the reader can default the
/// field; returns `Err` if it exists with the wrong Arrow type.
///
/// Used so that older Parquet files written before an `i32` column
/// existed (see Phase 0 PR4 / NeMo Guardrails) still read cleanly.
pub fn optional_i32_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> anyhow::Result<Option<&'a Int32Array>> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(Some)
            .ok_or_else(|| anyhow!("column {} is not Int32Array", name)),
    }
}

/// Convert an Arrow `RecordBatch` back into a `Vec<Span>`.
///
/// This is the inverse of [`spans_to_record_batch`], used by the read path
/// after querying Parquet files with DataFusion.
///
/// Required columns from the original 45-field baseline schema must be
/// present; missing or wrong-typed required columns return an error.
/// Columns looked up via [`optional_string_col`] / [`optional_i16_col`] /
/// [`optional_i32_col`] may be absent — they default to the empty string
/// or 0 per row. This tolerance lets the read path consume older Parquet
/// files written before new columns were added (see
/// `TECH-SPEC-PHASE-0.md` §4.5). Today the Phase 0 +6 Guardrails columns
/// (`rail_type`, `rail_name`, `rail_stop`, `action_name`,
/// `action_has_llm_calls`, `action_llm_calls_count`) all use this path.
pub fn record_batch_to_spans(batch: &RecordBatch) -> anyhow::Result<Vec<Span>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(vec![]);
    }

    // Helper to extract a required StringArray column.
    macro_rules! str_col {
        ($name:expr) => {{
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("column {} is not StringArray", $name))?
        }};
    }

    // Helper to extract a required Int64Array column.
    macro_rules! i64_col {
        ($name:expr) => {{
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("column {} is not Int64Array", $name))?
        }};
    }

    // Helper to extract a required Int32Array column.
    macro_rules! i32_col {
        ($name:expr) => {{
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow!("column {} is not Int32Array", $name))?
        }};
    }

    // Helper to extract a required Float64Array column.
    macro_rules! f64_col {
        ($name:expr) => {{
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("column {} is not Float64Array", $name))?
        }};
    }

    // Extract all columns upfront to avoid repeated lookups.
    let trace_id_col = str_col!("trace_id");
    let span_id_col = str_col!("span_id");
    let parent_span_id_col = str_col!("parent_span_id");
    let timestamp_col = i64_col!("timestamp");
    let duration_ns_col = i64_col!("duration_ns");
    let workspace_id_col = str_col!("workspace_id");
    let service_name_col = str_col!("service_name");
    let span_name_col = str_col!("span_name");
    let span_kind_col = str_col!("span_kind");
    let span_type_col = str_col!("span_type");
    let status_code_col = str_col!("status_code");
    let status_message_col = str_col!("status_message");
    let invocation_id_col = str_col!("invocation_id");
    let session_id_col = str_col!("session_id");
    let user_id_col = str_col!("user_id");
    let agent_name_col = str_col!("agent_name");
    let agent_type_col = str_col!("agent_type");
    let llm_model_col = str_col!("llm_model");
    let llm_input_col = str_col!("llm_input");
    let llm_output_col = str_col!("llm_output");
    let prompt_tokens_col = i32_col!("prompt_tokens");
    let completion_tokens_col = i32_col!("completion_tokens");
    let total_tokens_col = i32_col!("total_tokens");
    let prompt_cost_usd_col = f64_col!("prompt_cost_usd");
    let completion_cost_usd_col = f64_col!("completion_cost_usd");
    let total_cost_usd_col = f64_col!("total_cost_usd");
    let tool_name_col = str_col!("tool_name");
    let tool_call_id_col = str_col!("tool_call_id");
    // Guardrails columns (Phase 0 R0.2 – R0.4) are routed through the
    // optional-column helpers so older Parquet files written before these
    // columns existed still read cleanly (missing columns default to empty
    // string / 0). See TECH-SPEC-PHASE-0.md §4.5.
    let rail_type_col = optional_string_col(batch, "rail_type")?;
    let rail_name_col = optional_string_col(batch, "rail_name")?;
    let rail_stop_col = optional_i16_col(batch, "rail_stop")?;
    let action_name_col = optional_string_col(batch, "action_name")?;
    let action_has_llm_calls_col = optional_i16_col(batch, "action_has_llm_calls")?;
    let action_llm_calls_count_col = optional_i32_col(batch, "action_llm_calls_count")?;
    // NeMo Phase 1 columns (R1.2 – R1.6). Same optional-column treatment as
    // the Guardrails block — Phase 0 (51-col) Parquet files predate these and
    // must still read cleanly.
    let workflow_run_id_col = optional_string_col(batch, "workflow_run_id")?;
    let framework_col = optional_string_col(batch, "framework")?;
    let llm_provider_col = optional_string_col(batch, "llm_provider")?;
    let llm_response_model_col = optional_string_col(batch, "llm_response_model")?;
    let events_col = optional_string_col(batch, "events")?;
    // Phase 4 columns. Optional-column path so existing 56-col Parquet files
    // (Phase 1 schema) keep reading cleanly: missing columns default to empty.
    let llm_cache_hit_col = optional_i16_col(batch, "llm_cache_hit")?;
    let llm_response_id_col = optional_string_col(batch, "llm_response_id")?;
    let environment_col = optional_string_col(batch, "environment")?;
    let links_col = optional_string_col(batch, "links")?;
    let resource_cpu_micros_col = i64_col!("resource_cpu_micros");
    let resource_memory_bytes_col = i64_col!("resource_memory_bytes");
    let resource_memory_peak_col = i64_col!("resource_memory_peak");
    // prompt_id / prompt_name use the optional-column path so older Parquet
    // files lacking these columns still read cleanly (default to empty string).
    // The 45-column schema test guarantees writers still emit them.
    let prompt_id_col = optional_string_col(batch, "prompt_id")?;
    let prompt_name_col = optional_string_col(batch, "prompt_name")?;
    let prompt_version_col = i32_col!("prompt_version");
    // completion_start_time is nullable
    let completion_start_time_col = batch
        .column_by_name("completion_start_time")
        .ok_or_else(|| anyhow!("missing column: completion_start_time"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("column completion_start_time is not Int64Array"))?;
    let time_to_first_token_ms_col = i32_col!("time_to_first_token_ms");
    let agent_version_col = str_col!("agent_version");
    let sdk_version_col = str_col!("sdk_version");
    let level_col = str_col!("level");
    // Database (Phase 4 Gap #46 - Database Observability)
    let db_system_col = optional_string_col(batch, "db_system_name")?;
    let db_namespace_col = optional_string_col(batch, "db_namespace")?;
    let db_operation_name_col = optional_string_col(batch, "db_operation_name")?;
    let db_query_text_col = optional_string_col(batch, "db_query_text")?;
    let db_query_summary_col = optional_string_col(batch, "db_query_summary")?;
    let db_collection_name_col = optional_string_col(batch, "db_collection_name")?;
    let db_response_status_code_col = optional_string_col(batch, "db_response_status_code")?;
    let model_parameters_col = str_col!("model_parameters");
    let attributes_col = str_col!("attributes");
    let created_at_col = i64_col!("created_at");
    let updated_at_col = i64_col!("updated_at");
    let is_deleted_col = batch
        .column_by_name("is_deleted")
        .ok_or_else(|| anyhow!("missing column: is_deleted"))?
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| anyhow!("column is_deleted is not Int16Array"))?;

    let mut spans = Vec::with_capacity(n);
    for i in 0..n {
        spans.push(Span {
            trace_id: trace_id_col.value(i).to_string(),
            span_id: span_id_col.value(i).to_string(),
            parent_span_id: parent_span_id_col.value(i).to_string(),
            timestamp: timestamp_col.value(i),
            duration_ns: duration_ns_col.value(i),
            workspace_id: workspace_id_col.value(i).to_string(),
            service_name: service_name_col.value(i).to_string(),
            span_name: span_name_col.value(i).to_string(),
            span_kind: span_kind_col.value(i).to_string(),
            span_type: span_type_col.value(i).to_string(),
            status_code: status_code_col.value(i).to_string(),
            status_message: status_message_col.value(i).to_string(),
            invocation_id: invocation_id_col.value(i).to_string(),
            session_id: session_id_col.value(i).to_string(),
            user_id: user_id_col.value(i).to_string(),
            agent_name: agent_name_col.value(i).to_string(),
            agent_type: agent_type_col.value(i).to_string(),
            llm_model: llm_model_col.value(i).to_string(),
            llm_input: llm_input_col.value(i).to_string(),
            llm_output: llm_output_col.value(i).to_string(),
            prompt_tokens: prompt_tokens_col.value(i),
            completion_tokens: completion_tokens_col.value(i),
            total_tokens: total_tokens_col.value(i),
            prompt_cost_usd: prompt_cost_usd_col.value(i),
            completion_cost_usd: completion_cost_usd_col.value(i),
            total_cost_usd: total_cost_usd_col.value(i),
            tool_name: tool_name_col.value(i).to_string(),
            tool_call_id: tool_call_id_col.value(i).to_string(),
            rail_type: rail_type_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            rail_name: rail_name_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            rail_stop: rail_stop_col.map(|c| c.value(i)).unwrap_or(0),
            action_name: action_name_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            action_has_llm_calls: action_has_llm_calls_col.map(|c| c.value(i)).unwrap_or(0),
            action_llm_calls_count: action_llm_calls_count_col.map(|c| c.value(i)).unwrap_or(0),
            workflow_run_id: workflow_run_id_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            framework: framework_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            llm_provider: llm_provider_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            llm_response_model: llm_response_model_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            // events: legacy files had no column; default to "[]" not "" so
            // downstream JSON consumers can deserialize without a special case.
            events: events_col
                .map(|c| c.value(i).to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "[]".to_string()),
            // Phase 4 polish columns — same legacy-tolerant pattern.
            // Legacy files predate the column → unknown (-1), not a miss (0),
            // matching the tri-state Span default.
            llm_cache_hit: llm_cache_hit_col.map(|c| c.value(i)).unwrap_or(-1),
            llm_response_id: llm_response_id_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            environment: environment_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            // links: JSON column; legacy files default to "[]" not "" so
            // downstream consumers can deserialize uniformly.
            links: links_col
                .map(|c| c.value(i).to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "[]".to_string()),
            resource_cpu_micros: resource_cpu_micros_col.value(i),
            resource_memory_bytes: resource_memory_bytes_col.value(i),
            resource_memory_peak: resource_memory_peak_col.value(i),
            prompt_id: prompt_id_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            prompt_name: prompt_name_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            prompt_version: prompt_version_col.value(i),
            completion_start_time: if completion_start_time_col.is_null(i) {
                None
            } else {
                Some(completion_start_time_col.value(i))
            },
            time_to_first_token_ms: time_to_first_token_ms_col.value(i),
            agent_version: agent_version_col.value(i).to_string(),
            sdk_version: sdk_version_col.value(i).to_string(),
            level: level_col.value(i).to_string(),
            db_system_name: db_system_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_namespace: db_namespace_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_operation_name: db_operation_name_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_query_text: db_query_text_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_query_summary: db_query_summary_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_collection_name: db_collection_name_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            db_response_status_code: db_response_status_code_col
                .map(|c| c.value(i).to_string())
                .unwrap_or_default(),
            model_parameters: model_parameters_col.value(i).to_string(),
            attributes: attributes_col.value(i).to_string(),
            created_at: created_at_col.value(i),
            updated_at: updated_at_col.value(i),
            is_deleted: is_deleted_col.value(i),
        });
    }

    Ok(spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_span(trace_id: &str, workspace_id: &str) -> Span {
        Span {
            trace_id: trace_id.to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            workspace_id: workspace_id.to_string(),
            ..Span::default()
        }
    }

    #[test]
    fn test_span_arrow_schema_field_count() {
        let schema = span_arrow_schema();
        // 66 fields total: 45 baseline + 6 Phase 0 Guardrails + 5 Phase 1
        // (workflow_run_id, framework, llm_provider, llm_response_model, events)
        // + 4 Phase 4 (llm_cache_hit, llm_response_id, environment, links) - 1 workspace_id refactor.
        // + 7 Database Phase 4 PR2 (db_system_name, db_namespace, db_operation_name, db_query_text, db_query_summary, db_collection_name, db_response_status_code)
        // Per OQ7/D-G3 (single release), the schema-count test asserts the
        // final 66-col layout; multi-generation regression tests are dropped.
        assert_eq!(schema.fields().len(), 66);
    }

    #[test]
    fn test_spans_to_record_batch_empty() {
        let batch = spans_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 66);
    }

    #[test]
    fn test_round_trip() {
        let spans = vec![
            make_span("trace-001", "proj-001"),
            make_span("trace-002", "proj-001"),
        ];

        let batch = spans_to_record_batch(&spans).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let recovered = record_batch_to_spans(&batch).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].trace_id, "trace-001");
        assert_eq!(recovered[1].trace_id, "trace-002");
        assert_eq!(recovered[0].workspace_id, "proj-001");
    }

    #[test]
    fn test_round_trip_nullable_completion_start_time() {
        let span = Span {
            trace_id: "t1".to_string(),
            completion_start_time: Some(1_700_000_000_000_000),
            ..Span::default()
        };
        let span_no_cst = Span {
            trace_id: "t2".to_string(),
            completion_start_time: None,
            ..Span::default()
        };

        let batch = spans_to_record_batch(&[span, span_no_cst]).unwrap();
        let recovered = record_batch_to_spans(&batch).unwrap();

        assert_eq!(
            recovered[0].completion_start_time,
            Some(1_700_000_000_000_000)
        );
        assert_eq!(recovered[1].completion_start_time, None);
    }

    #[test]
    fn test_record_batch_to_spans_tolerates_missing_columns() {
        // Build a batch from the canonical writer, then drop two columns
        // (prompt_id, prompt_name) that the reader routes through
        // optional_string_col. The resulting narrower batch must still read
        // successfully, with the absent fields defaulting to empty strings.
        let spans = vec![
            Span {
                trace_id: "t1".to_string(),
                prompt_id: "should-be-dropped".to_string(),
                prompt_name: "should-also-be-dropped".to_string(),
                ..Span::default()
            },
            Span {
                trace_id: "t2".to_string(),
                ..Span::default()
            },
        ];
        let full_batch = spans_to_record_batch(&spans).unwrap();

        // Construct a narrower schema + column set with prompt_id and
        // prompt_name dropped, simulating an older Parquet file written
        // before those columns existed.
        let drop_names = ["prompt_id", "prompt_name"];
        let kept_fields: Vec<_> = full_batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !drop_names.contains(&f.name().as_str()))
            .cloned()
            .collect();
        let kept_columns: Vec<ArrayRef> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if drop_names.contains(&f.name().as_str()) {
                    None
                } else {
                    Some(full_batch.column(i).clone())
                }
            })
            .collect();
        let narrow_schema = Arc::new(Schema::new(kept_fields));
        let narrow_batch = RecordBatch::try_new(narrow_schema, kept_columns).unwrap();
        // Two columns dropped from the canonical batch.
        assert_eq!(
            narrow_batch.num_columns(),
            span_arrow_schema().fields().len() - 2
        );

        // Reader tolerates the absent columns and defaults them to empty strings.
        let recovered = record_batch_to_spans(&narrow_batch).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].trace_id, "t1");
        assert_eq!(recovered[0].prompt_id, "");
        assert_eq!(recovered[0].prompt_name, "");
        assert_eq!(recovered[1].trace_id, "t2");
        assert_eq!(recovered[1].prompt_id, "");
        assert_eq!(recovered[1].prompt_name, "");
    }

    #[test]
    fn test_record_batch_to_spans_tolerates_45_col_legacy_file() {
        // Simulate a Parquet file written before the Phase 0 PR4 +6
        // Guardrails columns were added: drop the 6 new columns from a
        // canonically-written batch and ensure the reader still produces
        // valid Span rows with the new fields defaulted to "" / 0.
        let spans = vec![
            Span {
                trace_id: "t1".to_string(),
                rail_type: "should-be-dropped".to_string(),
                rail_name: "input_check".to_string(),
                rail_stop: 1,
                action_name: "self_check_input".to_string(),
                action_has_llm_calls: 1,
                action_llm_calls_count: 3,
                ..Span::default()
            },
            Span {
                trace_id: "t2".to_string(),
                ..Span::default()
            },
        ];
        let full_batch = spans_to_record_batch(&spans).unwrap();

        let drop_names = [
            "rail_type",
            "rail_name",
            "rail_stop",
            "action_name",
            "action_has_llm_calls",
            "action_llm_calls_count",
        ];
        let kept_fields: Vec<_> = full_batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !drop_names.contains(&f.name().as_str()))
            .cloned()
            .collect();
        let kept_columns: Vec<ArrayRef> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if drop_names.contains(&f.name().as_str()) {
                    None
                } else {
                    Some(full_batch.column(i).clone())
                }
            })
            .collect();
        let narrow_schema = Arc::new(Schema::new(kept_fields));
        let narrow_batch = RecordBatch::try_new(narrow_schema, kept_columns).unwrap();
        // Six Guardrails columns dropped from the canonical batch.
        assert_eq!(
            narrow_batch.num_columns(),
            span_arrow_schema().fields().len() - 6
        );

        let recovered = record_batch_to_spans(&narrow_batch).unwrap();
        assert_eq!(recovered.len(), 2);
        // First span: dropped Guardrails columns default to empty / 0.
        assert_eq!(recovered[0].trace_id, "t1");
        assert_eq!(recovered[0].rail_type, "");
        assert_eq!(recovered[0].rail_name, "");
        assert_eq!(recovered[0].rail_stop, 0);
        assert_eq!(recovered[0].action_name, "");
        assert_eq!(recovered[0].action_has_llm_calls, 0);
        assert_eq!(recovered[0].action_llm_calls_count, 0);
        // Second span: defaults all the way through.
        assert_eq!(recovered[1].trace_id, "t2");
        assert_eq!(recovered[1].rail_type, "");
        assert_eq!(recovered[1].rail_stop, 0);
        assert_eq!(recovered[1].action_llm_calls_count, 0);
    }

    /// Phase 4 PR1: the four new columns (llm_cache_hit, llm_response_id,
    /// environment, links) round-trip through the writer + reader.
    #[test]
    fn test_phase4_columns_round_trip() {
        let span = Span {
            trace_id: "t1".to_string(),
            llm_cache_hit: 1,
            llm_response_id: "chatcmpl-abc123".to_string(),
            environment: "production".to_string(),
            links: r#"[{"trace_id":"aaaa","span_id":"bbbb"}]"#.to_string(),
            ..Span::default()
        };
        let batch = spans_to_record_batch(&[span]).unwrap();
        let recovered = record_batch_to_spans(&batch).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].llm_cache_hit, 1);
        assert_eq!(recovered[0].llm_response_id, "chatcmpl-abc123");
        assert_eq!(recovered[0].environment, "production");
        assert_eq!(
            recovered[0].links,
            r#"[{"trace_id":"aaaa","span_id":"bbbb"}]"#
        );
    }

    /// Database PR: the seven new columns round-trip through writer + reader.
    #[test]
    fn test_db_columns_round_trip() {
        let span = Span {
            trace_id: "t1".to_string(),
            db_system_name: "postgresql".to_string(),
            db_namespace: "public".to_string(),
            db_operation_name: "SELECT".to_string(),
            db_query_text: "SELECT * FROM users".to_string(),
            db_query_summary: "SELECT users".to_string(),
            db_collection_name: "users".to_string(),
            db_response_status_code: "0".to_string(),
            ..Span::default()
        };
        let batch = spans_to_record_batch(&[span]).unwrap();
        let recovered = record_batch_to_spans(&batch).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].db_system_name, "postgresql");
        assert_eq!(recovered[0].db_namespace, "public");
        assert_eq!(recovered[0].db_operation_name, "SELECT");
        assert_eq!(recovered[0].db_query_text, "SELECT * FROM users");
        assert_eq!(recovered[0].db_query_summary, "SELECT users");
        assert_eq!(recovered[0].db_collection_name, "users");
        assert_eq!(recovered[0].db_response_status_code, "0");
    }

    /// All three `llm_cache_hit` states (unknown `-1`, miss `0`, hit `1`) must
    /// survive a writer → reader round-trip distinctly — a miss must not be
    /// confused with unknown.
    #[test]
    fn test_llm_cache_hit_tristate_round_trips() {
        let spans = vec![
            Span {
                trace_id: "unknown".to_string(),
                llm_cache_hit: -1,
                ..Span::default()
            },
            Span {
                trace_id: "miss".to_string(),
                llm_cache_hit: 0,
                ..Span::default()
            },
            Span {
                trace_id: "hit".to_string(),
                llm_cache_hit: 1,
                ..Span::default()
            },
        ];
        let batch = spans_to_record_batch(&spans).unwrap();
        let recovered = record_batch_to_spans(&batch).unwrap();
        assert_eq!(recovered[0].llm_cache_hit, -1, "unknown must stay unknown");
        assert_eq!(recovered[1].llm_cache_hit, 0, "explicit miss must stay 0");
        assert_eq!(recovered[2].llm_cache_hit, 1, "explicit hit must stay 1");
    }

    /// Phase 4 PR1 read-compat: a 56-col Parquet file (Phase 1 era) must
    /// still read cleanly. Drop the four new columns from a canonically
    /// written batch and verify the reader defaults them safely
    /// (`-1` unknown, `""`, `""`, `"[]"`).
    #[test]
    fn test_record_batch_to_spans_tolerates_56_col_legacy_file() {
        let spans = vec![
            Span {
                trace_id: "t1".to_string(),
                llm_cache_hit: 1,
                llm_response_id: "should-be-dropped".to_string(),
                environment: "prod-should-be-dropped".to_string(),
                links: r#"[{"x":1}]"#.to_string(),
                ..Span::default()
            },
            Span {
                trace_id: "t2".to_string(),
                ..Span::default()
            },
        ];
        let full_batch = spans_to_record_batch(&spans).unwrap();
        let drop_names = ["llm_cache_hit", "llm_response_id", "environment", "links"];
        let kept_fields: Vec<_> = full_batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !drop_names.contains(&f.name().as_str()))
            .cloned()
            .collect();
        let kept_columns: Vec<ArrayRef> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if drop_names.contains(&f.name().as_str()) {
                    None
                } else {
                    Some(full_batch.column(i).clone())
                }
            })
            .collect();
        let narrow_schema = Arc::new(Schema::new(kept_fields));
        let narrow_batch = RecordBatch::try_new(narrow_schema, kept_columns).unwrap();
        assert_eq!(
            narrow_batch.num_columns(),
            span_arrow_schema().fields().len() - 4
        );

        let recovered = record_batch_to_spans(&narrow_batch).unwrap();
        assert_eq!(recovered.len(), 2);
        // First span loses the four new columns; defaults are safe.
        assert_eq!(recovered[0].llm_cache_hit, -1);
        assert_eq!(recovered[0].llm_response_id, "");
        assert_eq!(recovered[0].environment, "");
        assert_eq!(recovered[0].links, "[]");
        // Second span: identical defaults all the way through.
        assert_eq!(recovered[1].llm_cache_hit, -1);
        assert_eq!(recovered[1].llm_response_id, "");
        assert_eq!(recovered[1].environment, "");
        assert_eq!(recovered[1].links, "[]");
    }

    /// Phase 4 PR2 read-compat: a 59-col Parquet file (pre-database schema) must
    /// still read cleanly. Drop the 7 new columns from a canonically
    /// written batch and verify the reader defaults them safely (`""`).
    #[test]
    fn test_record_batch_to_spans_tolerates_59_col_legacy_file() {
        let spans = vec![
            Span {
                trace_id: "t1".to_string(),
                db_system_name: "should-be-dropped".to_string(),
                ..Span::default()
            },
            Span {
                trace_id: "t2".to_string(),
                ..Span::default()
            },
        ];
        let full_batch = spans_to_record_batch(&spans).unwrap();
        let drop_names = [
            "db_system_name",
            "db_namespace",
            "db_operation_name",
            "db_query_text",
            "db_query_summary",
            "db_collection_name",
            "db_response_status_code",
        ];
        let kept_fields: Vec<_> = full_batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !drop_names.contains(&f.name().as_str()))
            .cloned()
            .collect();
        let kept_columns: Vec<ArrayRef> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if drop_names.contains(&f.name().as_str()) {
                    None
                } else {
                    Some(full_batch.column(i).clone())
                }
            })
            .collect();
        let narrow_schema = Arc::new(Schema::new(kept_fields));
        let narrow_batch = RecordBatch::try_new(narrow_schema, kept_columns).unwrap();
        assert_eq!(
            narrow_batch.num_columns(),
            span_arrow_schema().fields().len() - 7
        );

        let recovered = record_batch_to_spans(&narrow_batch).unwrap();
        assert_eq!(recovered.len(), 2);
        // First span loses the new columns; defaults are safe empty strings.
        assert_eq!(recovered[0].db_system_name, "");
        assert_eq!(recovered[0].db_namespace, "");
        assert_eq!(recovered[0].db_operation_name, "");
        assert_eq!(recovered[0].db_query_text, "");
        assert_eq!(recovered[0].db_query_summary, "");
        assert_eq!(recovered[0].db_collection_name, "");
        assert_eq!(recovered[0].db_response_status_code, "");
        // Second span: identical defaults all the way through.
        assert_eq!(recovered[1].db_system_name, "");
    }

    #[test]
    fn test_optional_string_col_returns_none_when_absent() {
        let spans = vec![make_span("t1", "p1")];
        let batch = spans_to_record_batch(&spans).unwrap();
        let result = optional_string_col(&batch, "definitely_not_a_real_column").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_optional_string_col_returns_some_when_present() {
        let spans = vec![make_span("t1", "p1")];
        let batch = spans_to_record_batch(&spans).unwrap();
        let result = optional_string_col(&batch, "trace_id").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value(0), "t1");
    }

    #[test]
    fn test_optional_string_col_errors_on_wrong_type() {
        let spans = vec![make_span("t1", "p1")];
        let batch = spans_to_record_batch(&spans).unwrap();
        // timestamp is Int64Array, not StringArray
        let result = optional_string_col(&batch, "timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_100_spans() {
        let spans: Vec<Span> = (0..100)
            .map(|i| make_span(&format!("t{i}"), "p1"))
            .collect();
        let batch = spans_to_record_batch(&spans).unwrap();
        assert_eq!(batch.num_rows(), 100);

        let recovered = record_batch_to_spans(&batch).unwrap();
        assert_eq!(recovered.len(), 100);
        for (i, s) in recovered.iter().enumerate() {
            assert_eq!(s.trace_id, format!("t{i}"));
        }
    }

    /// On-disk regression for the Phase 0 PR4 column add: a Parquet file
    /// written with the original 45-column schema must still read cleanly
    /// after the +6 rail/action columns land, with the new fields defaulting
    /// to empty strings / zero. Complements the in-memory
    /// `test_record_batch_to_spans_tolerates_45_col_legacy_file` by exercising
    /// the actual Parquet writer/reader pipeline through a tempdir file.
    #[test]
    fn test_on_disk_45_col_legacy_parquet_reads_on_51_col_code() {
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("legacy_45col.parquet");

        // Build a 51-column batch, then strip the 6 Phase 0 PR4 columns to
        // simulate a file written before the Guardrails fields existed.
        let spans = vec![
            Span {
                trace_id: "trace-legacy-1".to_string(),
                workspace_id: "proj-1".to_string(),
                status_code: "OK".to_string(),
                llm_model: "gpt-4".to_string(),
                prompt_tokens: 42,
                completion_tokens: 7,
                total_tokens: 49,
                // These six fields are intentionally written into the source
                // span; they must vanish when we narrow the batch below.
                rail_type: "should-vanish".to_string(),
                rail_name: "should-also-vanish".to_string(),
                rail_stop: 1,
                action_name: "should-also-also-vanish".to_string(),
                action_has_llm_calls: 1,
                action_llm_calls_count: 99,
                ..Span::default()
            },
            Span {
                trace_id: "trace-legacy-2".to_string(),
                workspace_id: "proj-1".to_string(),
                ..Span::default()
            },
        ];
        let full_batch = spans_to_record_batch(&spans).unwrap();

        let drop_names = [
            "rail_type",
            "rail_name",
            "rail_stop",
            "action_name",
            "action_has_llm_calls",
            "action_llm_calls_count",
        ];
        let kept_fields: Vec<_> = full_batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !drop_names.contains(&f.name().as_str()))
            .cloned()
            .collect();
        let kept_columns: Vec<ArrayRef> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                if drop_names.contains(&f.name().as_str()) {
                    None
                } else {
                    Some(full_batch.column(i).clone())
                }
            })
            .collect();
        let narrow_schema = Arc::new(Schema::new(kept_fields));
        let narrow_batch = RecordBatch::try_new(narrow_schema.clone(), kept_columns).unwrap();
        // Six Phase 0 PR4 Guardrails columns dropped from the canonical batch.
        let dropped_count = drop_names.len();
        let expected_narrow_cols = span_arrow_schema().fields().len() - dropped_count;
        assert_eq!(narrow_batch.num_columns(), expected_narrow_cols);

        // Write the narrow batch to a real Parquet file using the same
        // ArrowWriter the production write path uses.
        {
            let file = std::fs::File::create(&file_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, narrow_schema, None).unwrap();
            writer.write(&narrow_batch).unwrap();
            writer.close().unwrap();
        }

        // Read it back with the standard Parquet Arrow reader, then pass
        // through record_batch_to_spans on the current-schema code path.
        let file = std::fs::File::open(&file_path).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let on_disk_batch = reader.next().unwrap().unwrap();
        assert_eq!(on_disk_batch.num_columns(), expected_narrow_cols);
        // No second batch.
        assert!(reader.next().is_none());

        let recovered = record_batch_to_spans(&on_disk_batch).unwrap();
        assert_eq!(recovered.len(), 2);

        // Carried-over columns still round-trip.
        assert_eq!(recovered[0].trace_id, "trace-legacy-1");
        assert_eq!(recovered[0].workspace_id, "proj-1");
        assert_eq!(recovered[0].status_code, "OK");
        assert_eq!(recovered[0].llm_model, "gpt-4");
        assert_eq!(recovered[0].prompt_tokens, 42);
        assert_eq!(recovered[0].completion_tokens, 7);
        assert_eq!(recovered[0].total_tokens, 49);

        // The six new fields default cleanly — no panic, no error.
        assert_eq!(recovered[0].rail_type, "");
        assert_eq!(recovered[0].rail_name, "");
        assert_eq!(recovered[0].rail_stop, 0);
        assert_eq!(recovered[0].action_name, "");
        assert_eq!(recovered[0].action_has_llm_calls, 0);
        assert_eq!(recovered[0].action_llm_calls_count, 0);

        assert_eq!(recovered[1].trace_id, "trace-legacy-2");
        assert_eq!(recovered[1].rail_type, "");
        assert_eq!(recovered[1].rail_stop, 0);
        assert_eq!(recovered[1].action_llm_calls_count, 0);
    }

    // Note: the Phase 0 → Phase 1 multi-generation regression test
    // (`tolerates_51_col_phase0_file`) was removed per OQ7 / D-G3 — production
    // never reads a pre-59-col file under the single-release model. The
    // generic optional-column tolerance is still covered by
    // `tolerates_missing_columns`, the immediate-prior-generation case by
    // `tolerates_56_col_legacy_file`, and the on-disk + 45-col tests below.
}
