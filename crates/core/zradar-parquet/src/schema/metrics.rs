//! Arrow schema and RecordBatch ↔ Metric conversions.
//!
//! Maps every field in `zradar_models::Metric` to an Arrow column type.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zradar_models::Metric;

/// Return the Arrow schema for the Metric type.
pub fn metric_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        // Timing (nanoseconds)
        Field::new("timestamp", DataType::Int64, false),
        // Multi-tenancy
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("project_id", DataType::Utf8, false),
        // Values
        Field::new("value", DataType::Float64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("min", DataType::Float64, false),
        Field::new("max", DataType::Float64, false),
        // Labels / metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("agent_name", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("labels", DataType::Utf8, false),
    ])
}

/// Convert a slice of `Metric` rows into an Arrow `RecordBatch`.
pub fn metrics_to_record_batch(metrics: &[Metric]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(metric_arrow_schema());

    let metric_name = StringArray::from_iter_values(metrics.iter().map(|m| m.metric_name.as_str()));
    let metric_type = StringArray::from_iter_values(metrics.iter().map(|m| m.metric_type.as_str()));
    let timestamp: Int64Array = metrics.iter().map(|m| m.timestamp).collect();
    let tenant_id = StringArray::from_iter_values(metrics.iter().map(|m| m.tenant_id.as_str()));
    let project_id = StringArray::from_iter_values(metrics.iter().map(|m| m.project_id.as_str()));
    let value: Float64Array = metrics.iter().map(|m| m.value).collect();
    let count: Int64Array = metrics.iter().map(|m| m.count).collect();
    let sum: Float64Array = metrics.iter().map(|m| m.sum).collect();
    let min: Float64Array = metrics.iter().map(|m| m.min).collect();
    let max: Float64Array = metrics.iter().map(|m| m.max).collect();
    let service_name =
        StringArray::from_iter_values(metrics.iter().map(|m| m.service_name.as_str()));
    let agent_name = StringArray::from_iter_values(metrics.iter().map(|m| m.agent_name.as_str()));
    let user_id = StringArray::from_iter_values(metrics.iter().map(|m| m.user_id.as_str()));
    let session_id = StringArray::from_iter_values(metrics.iter().map(|m| m.session_id.as_str()));
    let labels = StringArray::from_iter_values(metrics.iter().map(|m| m.labels.as_str()));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(metric_name),
        Arc::new(metric_type),
        Arc::new(timestamp),
        Arc::new(tenant_id),
        Arc::new(project_id),
        Arc::new(value),
        Arc::new(count),
        Arc::new(sum),
        Arc::new(min),
        Arc::new(max),
        Arc::new(service_name),
        Arc::new(agent_name),
        Arc::new(user_id),
        Arc::new(session_id),
        Arc::new(labels),
    ];

    RecordBatch::try_new(schema, columns).context("Failed to construct Metric RecordBatch")
}

/// Convert an Arrow `RecordBatch` back into a `Vec<Metric>`.
pub fn record_batch_to_metrics(batch: &RecordBatch) -> anyhow::Result<Vec<Metric>> {
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
    macro_rules! f64_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow!("missing column: {}", $name))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("column {} is not Float64Array", $name))?
        };
    }

    let metric_name_col = str_col!("metric_name");
    let metric_type_col = str_col!("metric_type");
    let timestamp_col = i64_col!("timestamp");
    let tenant_id_col = str_col!("tenant_id");
    let project_id_col = str_col!("project_id");
    let value_col = f64_col!("value");
    let count_col = i64_col!("count");
    let sum_col = f64_col!("sum");
    let min_col = f64_col!("min");
    let max_col = f64_col!("max");
    let service_name_col = str_col!("service_name");
    let agent_name_col = str_col!("agent_name");
    let user_id_col = str_col!("user_id");
    let session_id_col = str_col!("session_id");
    let labels_col = str_col!("labels");

    let mut metrics = Vec::with_capacity(n);
    for i in 0..n {
        metrics.push(Metric {
            metric_name: metric_name_col.value(i).to_string(),
            metric_type: metric_type_col.value(i).to_string(),
            timestamp: timestamp_col.value(i),
            tenant_id: tenant_id_col.value(i).to_string(),
            project_id: project_id_col.value(i).to_string(),
            value: value_col.value(i),
            count: count_col.value(i),
            sum: sum_col.value(i),
            min: min_col.value(i),
            max: max_col.value(i),
            service_name: service_name_col.value(i).to_string(),
            agent_name: agent_name_col.value(i).to_string(),
            user_id: user_id_col.value(i).to_string(),
            session_id: session_id_col.value(i).to_string(),
            labels: labels_col.value(i).to_string(),
        });
    }
    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metric(name: &str, project_id: &str) -> Metric {
        Metric {
            metric_name: name.to_string(),
            project_id: project_id.to_string(),
            ..Metric::default()
        }
    }

    #[test]
    fn test_metric_arrow_schema_field_count() {
        let schema = metric_arrow_schema();
        assert_eq!(schema.fields().len(), 15);
    }

    #[test]
    fn test_metrics_to_record_batch_empty() {
        let batch = metrics_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 15);
    }

    #[test]
    fn test_round_trip() {
        let metrics = vec![
            make_metric("requests.total", "proj-001"),
            make_metric("latency.p99", "proj-001"),
        ];
        let batch = metrics_to_record_batch(&metrics).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let recovered = record_batch_to_metrics(&batch).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].metric_name, "requests.total");
        assert_eq!(recovered[1].metric_name, "latency.p99");
    }
}
