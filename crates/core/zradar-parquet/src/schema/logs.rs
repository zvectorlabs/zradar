//! Arrow schema and RecordBatch ↔ LogRecord conversions.
//!
//! Maps every field in `zradar_models::LogRecord` to an Arrow column type.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zradar_models::LogRecord;

/// Return the Arrow schema for the LogRecord type.
pub fn log_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("id", DataType::Utf8, false),
        // Timing (nanoseconds)
        Field::new("timestamp", DataType::Int64, false),
        // Multi-tenancy
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("project_id", DataType::Utf8, false),
        // Trace correlation
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        // Severity
        Field::new("severity", DataType::Utf8, false),
        // Service metadata
        Field::new("service_name", DataType::Utf8, false),
        // Log message
        Field::new("message", DataType::Utf8, false),
        // JSON attributes
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource", DataType::Utf8, false),
        // Agent context
        Field::new("agent_name", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        // Lifecycle
        Field::new("created_at", DataType::Int64, false),
    ])
}

/// Convert a slice of `LogRecord` rows into an Arrow `RecordBatch`.
pub fn logs_to_record_batch(logs: &[LogRecord]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(log_arrow_schema());

    let id = StringArray::from_iter_values(logs.iter().map(|l| l.id.as_str()));
    let timestamp: Int64Array = logs.iter().map(|l| l.timestamp).collect();
    let tenant_id = StringArray::from_iter_values(logs.iter().map(|l| l.tenant_id.as_str()));
    let project_id = StringArray::from_iter_values(logs.iter().map(|l| l.project_id.as_str()));
    let trace_id = StringArray::from_iter_values(logs.iter().map(|l| l.trace_id.as_str()));
    let span_id = StringArray::from_iter_values(logs.iter().map(|l| l.span_id.as_str()));
    let severity = StringArray::from_iter_values(logs.iter().map(|l| l.severity.as_str()));
    let service_name = StringArray::from_iter_values(logs.iter().map(|l| l.service_name.as_str()));
    let message = StringArray::from_iter_values(logs.iter().map(|l| l.message.as_str()));
    let attributes = StringArray::from_iter_values(logs.iter().map(|l| l.attributes.as_str()));
    let resource = StringArray::from_iter_values(logs.iter().map(|l| l.resource.as_str()));
    let agent_name = StringArray::from_iter_values(logs.iter().map(|l| l.agent_name.as_str()));
    let session_id = StringArray::from_iter_values(logs.iter().map(|l| l.session_id.as_str()));
    let user_id = StringArray::from_iter_values(logs.iter().map(|l| l.user_id.as_str()));
    let created_at: Int64Array = logs.iter().map(|l| l.created_at).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id),
        Arc::new(timestamp),
        Arc::new(tenant_id),
        Arc::new(project_id),
        Arc::new(trace_id),
        Arc::new(span_id),
        Arc::new(severity),
        Arc::new(service_name),
        Arc::new(message),
        Arc::new(attributes),
        Arc::new(resource),
        Arc::new(agent_name),
        Arc::new(session_id),
        Arc::new(user_id),
        Arc::new(created_at),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to construct LogRecord RecordBatch")
}

/// Convert an Arrow `RecordBatch` back into a `Vec<LogRecord>`.
pub fn record_batch_to_logs(batch: &RecordBatch) -> anyhow::Result<Vec<LogRecord>> {
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
                .ok_or_else(|| anyhow!("column {} is not StringArray", $name))?
        };
    }
    macro_rules! i64_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("column {} is not Int64Array", $name))?
        };
    }

    let id_col = str_col!("id");
    let timestamp_col = i64_col!("timestamp");
    let tenant_id_col = str_col!("tenant_id");
    let project_id_col = str_col!("project_id");
    let trace_id_col = str_col!("trace_id");
    let span_id_col = str_col!("span_id");
    let severity_col = str_col!("severity");
    let service_name_col = str_col!("service_name");
    let message_col = str_col!("message");
    let attributes_col = str_col!("attributes");
    let resource_col = str_col!("resource");
    let agent_name_col = str_col!("agent_name");
    let session_id_col = str_col!("session_id");
    let user_id_col = str_col!("user_id");
    let created_at_col = i64_col!("created_at");

    let mut logs = Vec::with_capacity(n);
    for i in 0..n {
        logs.push(LogRecord {
            id: id_col.value(i).to_string(),
            timestamp: timestamp_col.value(i),
            tenant_id: tenant_id_col.value(i).to_string(),
            project_id: project_id_col.value(i).to_string(),
            trace_id: trace_id_col.value(i).to_string(),
            span_id: span_id_col.value(i).to_string(),
            severity: severity_col.value(i).to_string(),
            service_name: service_name_col.value(i).to_string(),
            message: message_col.value(i).to_string(),
            attributes: attributes_col.value(i).to_string(),
            resource: resource_col.value(i).to_string(),
            agent_name: agent_name_col.value(i).to_string(),
            session_id: session_id_col.value(i).to_string(),
            user_id: user_id_col.value(i).to_string(),
            created_at: created_at_col.value(i),
        });
    }
    Ok(logs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_log(id: &str, project_id: &str) -> LogRecord {
        LogRecord {
            id: id.to_string(),
            project_id: project_id.to_string(),
            severity: "INFO".to_string(),
            ..LogRecord::default()
        }
    }

    #[test]
    fn test_log_arrow_schema_field_count() {
        let schema = log_arrow_schema();
        assert_eq!(schema.fields().len(), 15);
    }

    #[test]
    fn test_logs_to_record_batch_empty() {
        let batch = logs_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 15);
    }

    #[test]
    fn test_round_trip() {
        let logs = vec![
            make_log("log-001", "proj-001"),
            make_log("log-002", "proj-001"),
        ];
        let batch = logs_to_record_batch(&logs).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let recovered = record_batch_to_logs(&batch).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].id, "log-001");
        assert_eq!(recovered[1].id, "log-002");
        assert_eq!(recovered[0].severity, "INFO");
    }
}
