//! # zradar-traits
//!
//! Core trait definitions for zradar.
//!
//! This crate provides abstractions for:
//! - Authentication (`Authenticator`)
//! - Repository traits (`FileListRepository`, `TelemetryReader`, `TelemetryWriter`)
//! - Block storage (`BlockStorage`)

pub mod auth;
pub mod block_storage;
pub mod repositories;

pub use auth::Authenticator;
pub use block_storage::BlockStorage;

// Re-export repository traits
pub use repositories::{
    AnalyticsReader, AuditLogFilters, AuditLogPage, AuditLogRepository, FileListRepository,
    RetentionPolicyRepository, SettingsRepository, TelemetryReader, TelemetryWriter,
};

// Re-export entity types
pub use repositories::{
    AnalyticsDataPoint, AnalyticsQueryFilters, LogQueryFilters, MetricPoint, MetricQueryFilters,
    MetricSeriesFilters, MetricsSummary, PaginatedResponse, Pagination, SpanQueryFilters,
    TimeRange, TimeSeriesPoint, TraceQueryFilters, TraceSummary,
};
