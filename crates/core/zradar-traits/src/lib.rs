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
pub mod auth_resolution;
pub mod block_storage;
pub mod capability;
pub mod content_capture;
pub mod file_lease;
pub mod query_authorizer;
pub mod repositories;

pub use admin_authorizer::{AdminAuth, AdminAuthorizer};
pub use auth::Authenticator;
pub use auth_resolution::AuthResolution;
pub use block_storage::BlockStorage;
pub use capability::Capability;
pub use content_capture::{ContentCapturePolicy, NoopContentCapturePolicy};
pub use file_lease::{FileLease, FileLeaseRegistry};
pub use query_authorizer::{QueryAuth, QueryAuthorizer};

// Re-export repository traits
pub use repositories::{
    AnalyticsReader, AuditLogFilters, AuditLogPage, AuditLogRepository, CachedSettingsRepository,
    FileListRepository, SettingsRepository, StorageUsageDailySnapshot, StorageUsageDelta,
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

// Domain errors — transport-agnostic
pub mod errors;
pub use errors::ServiceError;

// Auth context — service-layer identity envelope
pub mod auth_context;
pub use auth_context::AuthContext;

// Service-layer traits
pub mod services;
pub use services::{
    AnalyticsQueryService, AuditQueryService, PolicyAdminService, RetentionService,
    SettingsAdminService, TelemetryQueryService,
};
