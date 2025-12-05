//! Repository trait definitions
//!
//! These traits define the interfaces for data persistence.
//! Implementations can be PostgreSQL, ClickHouse, or any other backend.

pub mod users;
pub mod organizations;
pub mod projects;
pub mod api_keys;
pub mod roles;
pub mod telemetry;
pub mod scores;
pub mod audit;

// Re-export all traits
pub use users::UserRepository;
pub use organizations::OrganizationRepository;
pub use projects::ProjectRepository;
pub use api_keys::ApiKeyRepository;
pub use roles::RoleRepository;
pub use telemetry::{TelemetryWriter, TelemetryReader};
pub use scores::{ScoreRepository, ScoreSummary};
pub use audit::AuditLogger;

// Re-export entity types from each module
pub use users::{User, UpdateUserRequest};
pub use organizations::{
    Organization, OrganizationMember, OrganizationWithRole,
    CreateOrganizationRequest, UpdateOrganizationRequest, AddMemberRequest,
};
pub use projects::{
    Project, ProjectMember, ProjectWithRole,
    CreateProjectRequest, UpdateProjectRequest, AddProjectMemberRequest,
};
pub use api_keys::{
    ApiKey, CreateApiKeyRequest, CreateApiKeyResponse, ApiKeyResponse,
};
pub use roles::{
    PermissionDefinition, CustomRole,
    CreateCustomRoleRequest, UpdateCustomRoleRequest,
    RiskAssessment, PermissionInfo,
};
pub use telemetry::{
    TraceQueryFilters, SpanQueryFilters, TraceSummary, PaginatedResponse, Pagination, TimeRange,
};
pub use audit::{AuditLog, AuditEvent, AuditStatus};
