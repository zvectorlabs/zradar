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

pub mod job_queue;
pub mod block_storage;
pub mod repositories;

// Re-export job queue types
pub use job_queue::{JobQueue, Job, JobStatus, JobType, QueueStats, generate_sharded_path};
pub use block_storage::BlockStorage;

// Re-export repository traits
pub use repositories::{
    UserRepository,
    OrganizationRepository,
    ProjectRepository,
    ApiKeyRepository,
    RoleRepository,
    TelemetryWriter,
    TelemetryReader,
    ScoreRepository,
    AuditLogger,
};

// Re-export entity types
pub use repositories::{
    // Users
    User, UpdateUserRequest,
    // Organizations  
    Organization, OrganizationMember, OrganizationWithRole,
    CreateOrganizationRequest, UpdateOrganizationRequest, AddMemberRequest,
    // Projects
    Project, ProjectMember, ProjectWithRole,
    CreateProjectRequest, UpdateProjectRequest, AddProjectMemberRequest,
    // API Keys
    ApiKey, CreateApiKeyRequest, CreateApiKeyResponse, ApiKeyResponse,
    // Roles
    PermissionDefinition, CustomRole,
    CreateCustomRoleRequest, UpdateCustomRoleRequest,
    RiskAssessment, PermissionInfo,
    // Telemetry
    TraceQueryFilters, SpanQueryFilters, TraceSummary, PaginatedResponse, Pagination, TimeRange,
    // Scores
    ScoreSummary,
    // Audit
    AuditLog, AuditEvent, AuditStatus,
};
