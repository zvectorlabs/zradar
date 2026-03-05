//! # zradar-traits
//!
//! Core trait definitions for zradar architecture.
//!
//! This crate provides abstractions for:
//! - Repository traits (Users, Organizations, Projects, API Keys, Roles)
//! - Telemetry traits (Reader, Writer)
//! - Score repository
//! - Audit logging
//! - Job queue implementations
//! - Block storage implementations
//!
//! ## Architecture
//!
//! This is a Layer 1 (core) crate that defines interfaces.
//! Plugins implement these traits (PostgreSQL, ClickHouse, etc.)
//! Services depend only on traits, not implementations.

pub mod block_storage;
pub mod job_queue;
pub mod repositories;

// Re-export job queue types
pub use block_storage::BlockStorage;
pub use job_queue::{Job, JobQueue, JobStatus, JobType, QueueStats, generate_sharded_path};

// Re-export repository traits
pub use repositories::{
    AnalyticsReader, ApiKeyRepository, AuditLogger, FileListRepository, OrganizationRepository,
    ProjectRepository, RoleRepository, ScoreRepository, TelemetryReader, TelemetryWriter,
    UserRepository,
};

// Re-export entity types
pub use repositories::{
    AddMemberRequest,
    AddProjectMemberRequest,
    // API Keys
    ApiKey,
    ApiKeyResponse,
    AuditEvent,
    // Audit
    AuditLog,
    AuditStatus,
    CreateApiKeyRequest,
    CreateApiKeyResponse,
    CreateCustomRoleRequest,
    CreateOrganizationRequest,
    CreateProjectRequest,
    CustomRole,
    // Organizations
    Organization,
    OrganizationMember,
    OrganizationWithRole,
    PaginatedResponse,
    Pagination,
    // Roles
    PermissionDefinition,
    PermissionInfo,
    // Projects
    Project,
    ProjectMember,
    ProjectWithRole,
    RiskAssessment,
    // Scores
    ScoreSummary,
    // Telemetry
    LogQueryFilters,
    MetricPoint,
    MetricQueryFilters,
    MetricSeriesFilters,
    MetricsSummary,
    SpanQueryFilters,
    TimeRange,
    TimeSeriesPoint,
    TraceQueryFilters,
    TraceSummary,
    UpdateCustomRoleRequest,
    UpdateOrganizationRequest,
    UpdateProjectRequest,
    UpdateUserRequest,
    // Users
    User,
};
