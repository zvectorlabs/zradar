//! HTTP handlers for Admin API
//!
//! These handlers implement the REST API endpoints for zradar control plane.
//! They delegate to services in the domain modules.


// Re-export service types for router compatibility
pub use crate::users::AuthService;
pub use crate::organizations::OrganizationService;
pub use crate::projects::ProjectService;
pub use crate::api_keys::service::ApiKeyService;
pub use crate::roles::RoleService;
pub use crate::telemetry::QueryService;
pub use crate::scores::ScoresService;

