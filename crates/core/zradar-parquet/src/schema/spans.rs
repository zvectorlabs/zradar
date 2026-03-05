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
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("project_id", DataType::Utf8, false),
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
    let tenant_id = StringArray::from_iter_values(spans.iter().map(|s| s.tenant_id.as_str()));
    let project_id = StringArray::from_iter_values(spans.iter().map(|s| s.project_id.as_str()));
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
        Arc::new(tenant_id),
        Arc::new(project_id),
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
        Arc::new(model_parameters),
        Arc::new(attributes),
        Arc::new(created_at),
        Arc::new(updated_at),
        Arc::new(is_deleted),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to construct Span RecordBatch")
}

/// Convert an Arrow `RecordBatch` back into a `Vec<Span>`.
///
/// This is the inverse of [`spans_to_record_batch`], used by the read path
/// after querying Parquet files with DataFusion.
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
    let tenant_id_col = str_col!("tenant_id");
    let project_id_col = str_col!("project_id");
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
    let resource_cpu_micros_col = i64_col!("resource_cpu_micros");
    let resource_memory_bytes_col = i64_col!("resource_memory_bytes");
    let resource_memory_peak_col = i64_col!("resource_memory_peak");
    let prompt_id_col = str_col!("prompt_id");
    let prompt_name_col = str_col!("prompt_name");
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
            tenant_id: tenant_id_col.value(i).to_string(),
            project_id: project_id_col.value(i).to_string(),
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
            resource_cpu_micros: resource_cpu_micros_col.value(i),
            resource_memory_bytes: resource_memory_bytes_col.value(i),
            resource_memory_peak: resource_memory_peak_col.value(i),
            prompt_id: prompt_id_col.value(i).to_string(),
            prompt_name: prompt_name_col.value(i).to_string(),
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

    fn make_span(trace_id: &str, project_id: &str) -> Span {
        Span {
            trace_id: trace_id.to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            project_id: project_id.to_string(),
            ..Span::default()
        }
    }

    #[test]
    fn test_span_arrow_schema_field_count() {
        let schema = span_arrow_schema();
        // 45 fields total (all fields in the Span struct)
        assert_eq!(schema.fields().len(), 45);
    }

    #[test]
    fn test_spans_to_record_batch_empty() {
        let batch = spans_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 45);
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
        assert_eq!(recovered[0].project_id, "proj-001");
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
}
