//! Arrow schemas and RecordBatch conversions for zradar telemetry types.

pub mod logs;
pub mod metrics;
pub mod spans;

pub use logs::{log_arrow_schema, logs_to_record_batch, record_batch_to_logs};
pub use metrics::{metric_arrow_schema, metrics_to_record_batch, record_batch_to_metrics};
pub use spans::{record_batch_to_spans, span_arrow_schema, spans_to_record_batch};
