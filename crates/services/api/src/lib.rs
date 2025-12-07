//! # api
//!
//! REST API control plane for zradar - business logic and HTTP handlers.
//! No database implementations - those are in plugins.
//!
//! ## Architecture (Vertical Slicing)
//!
//! ```text
//! api/
//! ├── organizations/  # Organization domain (types, service, handlers)
//! ├── users/          # User/auth domain (types, service, handlers)
//! ├── projects/       # Project domain (types, service, handlers)
//! ├── api_keys/       # API key domain (types, service, handlers)
//! ├── roles/          # Role management domain (types, service, handlers)
//! ├── scores/         # Score/evaluation domain (types, service, handlers)
//! ├── telemetry/      # Query/telemetry domain (types, service, handlers)
//! ├── http/           # HTTP utilities (router, extractors)
//! ├── auth/           # Authentication (JWT, API keys)
//! ├── rbac/           # Role-based access control
//! ├── audit/          # Audit logging
//! ├── errors/         # Error types
//! └── permissions     # Permission validation
//! ```
//!
//! ## Database Access
//!
//! Services receive repositories via dependency injection:
//! ```ignore
//! use zradar_traits::{UserRepository, OrganizationRepository};
//! use zradar_plugin_postgres::repositories::*;
//!
//! let user_repo: Arc<dyn UserRepository> = Arc::new(PostgresUserRepository::new(client));
//! ```

// =============================================================================
// DOMAIN MODULES (Vertical Slicing)
// =============================================================================

/// Organization management domain
pub mod organizations;

/// User management and authentication domain
pub mod users;

/// Project management domain
pub mod projects;

/// API key management domain
pub mod api_keys;

/// Role management domain
pub mod roles;

/// Score/evaluation domain
pub mod scores;

/// Query/telemetry domain
pub mod telemetry;

// =============================================================================
// CROSS-CUTTING MODULES
// =============================================================================

/// HTTP layer - HTTP handlers and router
pub mod http;

/// Authentication (JWT, API keys)
pub mod auth;

/// Role-based access control
pub mod rbac;

/// Audit logging
pub mod audit;

/// Error types
pub mod errors;

/// Permission validation utilities
pub mod permissions;

// =============================================================================
// OLD MODULES (Deprecated - for backwards compatibility)
// =============================================================================

/// Domain layer - HTTP DTOs and API types (DEPRECATED: Use domain modules instead)
pub mod domain;

// =============================================================================
// RE-EXPORTS
// =============================================================================

// Errors
pub use errors::{ControlError, Result};

// RBAC
pub use rbac::{PermissionChecker, RbacService};

// Audit
pub use audit::AuditLogger;

// Re-export traits from zradar_traits for convenience
pub use zradar_traits::{
    ApiKeyRepository, OrganizationRepository, ProjectRepository, RoleRepository, ScoreRepository,
    TelemetryReader, TelemetryWriter, UserRepository,
};
