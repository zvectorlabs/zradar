//! Repository trait definitions

pub mod audit_log;
pub mod file_list;
pub mod retention_policy;
pub mod settings;
pub mod storage_usage;
pub mod telemetry;

pub use audit_log::{AuditLogFilters, AuditLogPage, AuditLogRepository};
pub use file_list::FileListRepository;
pub use retention_policy::RetentionPolicyRepository;
pub use settings::SettingsRepository;
pub use storage_usage::{StorageUsageDailySnapshot, StorageUsageDelta, StorageUsageRepository};
pub use telemetry::{AnalyticsReader, TelemetryReader, TelemetryWriter};

// Re-export entity types
pub use telemetry::{
    AnalyticsDataPoint, AnalyticsQueryFilters, LogQueryFilters, MetricPoint, MetricQueryFilters,
    MetricSeriesFilters, MetricsSummary, PaginatedResponse, Pagination, SpanQueryFilters,
    TimeRange, TimeSeriesPoint, TraceQueryFilters, TraceSummary,
};
