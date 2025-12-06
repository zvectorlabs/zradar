//! Admin API Router - Composes domain routers
//!
//! This router merges all domain-specific routers into a single API.
//! Each domain module defines its own routes in `<domain>/router.rs`.

use axum::Router;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::{TokenAuth, DefaultKeyGenerator};
use crate::users::{AuthService, UserRepository, handlers as auth_handlers};
use crate::organizations::{OrganizationService, handlers as org_handlers};
use crate::projects::{ProjectService, handlers as proj_handlers};
use crate::api_keys::{service::ApiKeyService, handlers as apikey_handlers};
use crate::roles::{RoleService, handlers as role_handlers};
use crate::telemetry::{QueryService, handlers as query_handlers};
use crate::scores::{ScoresService, handlers as score_handlers};

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(
        // Auth endpoints
        auth_handlers::register,
        auth_handlers::login,
        auth_handlers::refresh,
        auth_handlers::me,
        
        // Organization endpoints
        org_handlers::create_organization,
        org_handlers::list_organizations,
        org_handlers::get_organization,
        org_handlers::update_organization,
        org_handlers::delete_organization,
        org_handlers::add_organization_member,
        org_handlers::list_organization_members,
        org_handlers::remove_organization_member,
        
        // Project endpoints
        proj_handlers::create_project,
        proj_handlers::list_projects,
        proj_handlers::get_project,
        proj_handlers::update_project,
        proj_handlers::delete_project,
        proj_handlers::add_project_member,
        proj_handlers::list_project_members,
        proj_handlers::remove_project_member,
        
        // API Key endpoints
        apikey_handlers::create_api_key,
        apikey_handlers::list_api_keys,
        apikey_handlers::get_api_key,
        apikey_handlers::revoke_api_key,
        apikey_handlers::delete_api_key,
        
        // Role endpoints
        role_handlers::create_role,
        role_handlers::list_roles,
        role_handlers::get_custom_role,
        role_handlers::update_custom_role,
        role_handlers::delete_custom_role,
        role_handlers::list_permissions,
        
        // Query endpoints
        query_handlers::query_traces,
        query_handlers::get_trace,
        query_handlers::query_spans,
        query_handlers::get_analytics,
        query_handlers::get_top_endpoints,
        query_handlers::get_error_breakdown,
        
        // Scores endpoints
        score_handlers::create_score,
        score_handlers::get_trace_scores,
        score_handlers::get_trace_score_summary,
        score_handlers::get_session_scores,
        score_handlers::get_score_by_id,
        score_handlers::delete_score,
    ),
    components(
        schemas(
            crate::users::User,
            crate::users::UserResponse,
            crate::users::RegisterRequest,
            crate::users::LoginRequest,
            crate::users::AuthResponse,
            crate::users::RefreshResponse,
            crate::organizations::Organization,
            crate::organizations::OrganizationMember,
            crate::organizations::OrganizationWithRole,
            crate::organizations::CreateOrganizationRequest,
            crate::organizations::UpdateOrganizationRequest,
            crate::organizations::AddOrganizationMemberRequest,
            crate::projects::Project,
            crate::projects::ProjectMember,
            crate::projects::CreateProjectRequest,
            crate::projects::UpdateProjectRequest,
            crate::projects::AddProjectMemberRequest,
            crate::api_keys::ApiKey,
            crate::api_keys::ApiKeyResponse,
            crate::api_keys::CreateApiKeyRequest,
            crate::api_keys::CreateApiKeyResponse,
            crate::roles::CustomRole,
            crate::roles::CreateCustomRoleRequest,
            crate::roles::UpdateCustomRoleRequest,
            crate::roles::PermissionDefinition,
            crate::telemetry::TraceQueryFilters,
            crate::telemetry::SpanQueryFilters,
            crate::telemetry::AnalyticsQuery,
            crate::telemetry::TopNQuery,
            crate::telemetry::ErrorAnalyticsQuery,
            crate::telemetry::TraceSummary,
            crate::telemetry::TraceDetail,
            crate::telemetry::SpanDetail,
            crate::telemetry::AnalyticsResult,
            crate::telemetry::TopEndpoint,
            crate::telemetry::ErrorBreakdown,
            crate::telemetry::PaginatedResponse<crate::telemetry::TraceSummary>,
            crate::telemetry::PaginatedResponse<crate::telemetry::SpanDetail>,
            crate::scores::CreateScoreRequest,
            crate::scores::ScoreResponse,
            crate::scores::ScoreSummaryResponse,
            zradar_models::EvalDataType,
            zradar_models::EvalSource,
        )
    ),
    tags(
        (name = "Auth", description = "Authentication and user management"),
        (name = "Organizations", description = "Organization management"),
        (name = "Projects", description = "Project management"),
        (name = "ApiKeys", description = "API key management"),
        (name = "Roles", description = "Custom role management"),
        (name = "Query", description = "Query telemetry data (traces, spans)"),
        (name = "Analytics", description = "Analytics and aggregations"),
        (name = "Scores", description = "Evaluation scores management"),
    ),
    info(
        title = "ZRadar Admin API",
        version = "0.1.0",
        description = "Admin API for zradar - OpenTelemetry observability platform",
        contact(
            name = "zradar",
            url = "https://github.com/your-org/zradar"
        ),
        license(
            name = "Apache-2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    ),
    servers(
        (url = "http://localhost:8080", description = "Local development server"),
        (url = "https://api.zradar.io", description = "Production server")
    )
)]
pub struct ApiDoc;

/// Create the complete Admin API router by merging domain routers
pub fn create_admin_router(
    auth_service: Arc<AuthService>,
    org_service: Arc<OrganizationService>,
    project_service: Arc<ProjectService>,
    api_key_service: Arc<ApiKeyService<DefaultKeyGenerator>>,
    role_service: Arc<RoleService>,
    query_service: Arc<QueryService>,
    scores_service: Arc<ScoresService>,
    jwt_auth: Arc<dyn TokenAuth>,
    user_storage: Arc<dyn UserRepository>,
) -> Router {
    // Merge all domain routers
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui")
            .url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(crate::users::router::router(auth_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::organizations::router::router(org_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::projects::router::router(project_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::api_keys::router::router(api_key_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::roles::router::router(role_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::telemetry::router::router(query_service, jwt_auth.clone(), user_storage.clone()))
        .merge(crate::scores::router::router(scores_service, jwt_auth, user_storage))
}
