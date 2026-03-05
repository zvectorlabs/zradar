//! Repository trait definitions
//!
//! These traits define the interfaces for data persistence.
//! Implementations can be PostgreSQL, ClickHouse, or any other backend.

pub mod api_keys;
pub mod audit;
pub mod file_list;
pub mod organizations;
pub mod projects;
pub mod roles;
pub mod scores;
pub mod telemetry;
pub mod users;

// Re-export all traits
pub use api_keys::ApiKeyRepository;
pub use audit::AuditLogger;
pub use file_list::FileListRepository;
pub use organizations::OrganizationRepository;
pub use projects::ProjectRepository;
pub use roles::RoleRepository;
pub use scores::{ScoreRepository, ScoreSummary};
pub use telemetry::{AnalyticsReader, TelemetryReader, TelemetryWriter};
pub use users::UserRepository;

// Re-export entity types from each module
pub use api_keys::{ApiKey, ApiKeyResponse, CreateApiKeyRequest, CreateApiKeyResponse};
pub use audit::{AuditEvent, AuditLog, AuditStatus};
pub use organizations::{
    AddMemberRequest, CreateOrganizationRequest, Organization, OrganizationMember,
    OrganizationWithRole, UpdateOrganizationRequest,
};
pub use projects::{
    AddProjectMemberRequest, CreateProjectRequest, Project, ProjectMember, ProjectWithRole,
    UpdateProjectRequest,
};
pub use roles::{
    CreateCustomRoleRequest, CustomRole, PermissionDefinition, PermissionInfo, RiskAssessment,
    UpdateCustomRoleRequest,
};
pub use telemetry::{
    LogQueryFilters, MetricPoint, MetricQueryFilters, MetricSeriesFilters, MetricsSummary,
    PaginatedResponse, Pagination, SpanQueryFilters, TimeRange, TimeSeriesPoint,
    TraceQueryFilters, TraceSummary,
};
pub use users::{UpdateUserRequest, User};
