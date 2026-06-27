//! # zradar-traits
//!
//! Core trait definitions for zradar.
//!
//! This crate provides abstractions for:
//! - Authentication (`Authenticator`)
//! - Repository traits (`FileListRepository`, `TelemetryReader`, `TelemetryWriter`)
//! - Block storage (`BlockStorage`)
//! - Content capture policy (`ContentCapturePolicy`, `NoopContentCapturePolicy`)

pub mod admin_authorizer;
pub mod auth;
pub mod block_storage;
pub mod capability;
pub mod content_capture;
pub mod file_lease;
pub mod repositories;

pub use admin_authorizer::{AdminAuth, AdminAuthorizer};
pub use auth::Authenticator;
pub use block_storage::BlockStorage;
pub use capability::Capability;
pub use content_capture::{ContentCapturePolicy, NoopContentCapturePolicy};
pub use file_lease::{FileLease, FileLeaseRegistry};

// Re-export repository traits
pub use repositories::{
    AnalyticsReader, AuditLogFilters, AuditLogPage, AuditLogRepository, FileListRepository,
    RetentionPolicyRepository, SettingsRepository, StorageUsageDailySnapshot, StorageUsageDelta,
    StorageUsageRepository, TelemetryReader, TelemetryWriter,
};

// Re-export entity types
pub use repositories::{
    AnalyticsDataPoint, AnalyticsQueryFilters, GuardrailsAnalyticsFilters,
    GuardrailsAnalyticsResult, LogQueryFilters, MetricPoint, MetricQueryFilters,
    MetricSeriesFilters, MetricsSummary, PaginatedResponse, Pagination, RailNameStat,
    RailTypeBreakdown, SpanQueryFilters, TimeRange, TimeSeriesPoint, TraceQueryFilters,
    TraceSummary,
};
