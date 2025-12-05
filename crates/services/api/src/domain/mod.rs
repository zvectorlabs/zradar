//! Domain layer - HTTP DTOs and API types (DEPRECATED)
//!
//! This module is deprecated. Use the specific domain modules instead:
//! - `crate::organizations` for organization types
//! - `crate::users` for user types
//! - `crate::projects` for project types
//! - `crate::api_keys` for API key types
//! - `crate::roles` for role types
//! - `crate::scores` for score types
//! - `crate::telemetry` for telemetry/query types
//!
//! These re-exports are provided for backward compatibility only.

// Re-export from new modules for backward compatibility
pub use crate::users::{
    User, UserResponse, RegisterRequest, LoginRequest, AuthResponse, UserRepository
};

pub use crate::organizations::{
    Organization, OrganizationMember, OrganizationWithRole,
    CreateOrganizationRequest, UpdateOrganizationRequest, AddOrganizationMemberRequest,
    OrganizationRepository,
};

pub use crate::projects::{
    Project, ProjectMember, ProjectWithRole,
    CreateProjectRequest, UpdateProjectRequest, AddProjectMemberRequest,
    ProjectRepository,
};

pub use crate::api_keys::{
    ApiKey, CreateApiKeyRequest, CreateApiKeyResponse, ApiKeyResponse,
    ApiKeyRepository,
};

pub use crate::roles::{
    PermissionDefinition, CustomRole,
    CreateCustomRoleRequest, UpdateCustomRoleRequest,
    RiskAssessment, PermissionInfo,
    RoleRepository,
};

pub use crate::telemetry::{
    TraceQueryFilters, SpanQueryFilters, AnalyticsQuery, TopNQuery,
    TraceSummary, TraceDetail, SpanDetail,
    AnalyticsResult, TopEndpoint,
    TelemetryWriter,
};

pub use crate::scores::{
    CreateScoreRequest, ScoreResponse, ScoreSummaryResponse,
    ScoreRepository,
};

// Keep old submodules temporarily
pub mod organizations {
    pub use crate::organizations::*;
}

pub mod users {
    pub use crate::users::*;
}

pub mod projects {
    pub use crate::projects::*;
}

pub mod api_keys {
    pub use crate::api_keys::*;
}

pub mod roles {
    pub use crate::roles::*;
}

pub mod scores {
    pub use crate::scores::*;
}

pub mod telemetry {
    pub use crate::telemetry::*;
}
