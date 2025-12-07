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
    AuthResponse, LoginRequest, RegisterRequest, User, UserRepository, UserResponse,
};

pub use crate::organizations::{
    AddOrganizationMemberRequest, CreateOrganizationRequest, Organization, OrganizationMember,
    OrganizationRepository, OrganizationWithRole, UpdateOrganizationRequest,
};

pub use crate::projects::{
    AddProjectMemberRequest, CreateProjectRequest, Project, ProjectMember, ProjectRepository,
    ProjectWithRole, UpdateProjectRequest,
};

pub use crate::api_keys::{
    ApiKey, ApiKeyRepository, ApiKeyResponse, CreateApiKeyRequest, CreateApiKeyResponse,
};

pub use crate::roles::{
    CreateCustomRoleRequest, CustomRole, PermissionDefinition, PermissionInfo, RiskAssessment,
    RoleRepository, UpdateCustomRoleRequest,
};

pub use crate::telemetry::{
    AnalyticsQuery, AnalyticsResult, SpanDetail, SpanQueryFilters, TelemetryWriter, TopEndpoint,
    TopNQuery, TraceDetail, TraceQueryFilters, TraceSummary,
};

pub use crate::scores::{CreateScoreRequest, ScoreRepository, ScoreResponse, ScoreSummaryResponse};

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
