//! # zradar-traits
//!
//! Core trait definitions for zradar.
//!
//! This crate provides abstractions for:
//! - Authentication (`Authenticator`)
//! - Repository traits (`FileListRepository`, `TelemetryReader`, `TelemetryWriter`)
//! - Block storage (`BlockStorage`)

pub mod admin_authorizer;
pub mod auth;
pub mod block_storage;
pub mod capability;
pub mod repositories;

pub use admin_authorizer::{AdminAuth, AdminAuthorizer};
pub use auth::Authenticator;
pub use block_storage::BlockStorage;
pub use capability::Capability;

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
