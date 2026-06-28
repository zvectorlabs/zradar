//! Repository trait definitions

pub mod audit_log;
pub mod file_list;
pub mod settings;
pub mod storage_usage;
pub mod telemetry;

pub use audit_log::{AuditLogFilters, AuditLogPage, AuditLogRepository};
pub use file_list::FileListRepository;
pub use settings::{CachedSettingsRepository, SettingsRepository};
pub use storage_usage::{StorageUsageDailySnapshot, StorageUsageDelta, StorageUsageRepository};
pub use telemetry::{AnalyticsReader, TelemetryReader, TelemetryWriter};

// Re-export entity types
pub use telemetry::{
    AnalyticsDataPoint, AnalyticsQueryFilters, GuardrailsAnalyticsFilters,
    GuardrailsAnalyticsResult, LogQueryFilters, MetricPoint, MetricQueryFilters,
    MetricSeriesFilters, MetricsSummary, PaginatedResponse, Pagination, RailNameStat,
    RailTypeBreakdown, SpanQueryFilters, TimeRange, TimeSeriesPoint, TraceQueryFilters,
    TraceSummary,
};
