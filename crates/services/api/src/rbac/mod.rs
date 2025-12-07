//! RBAC shared module
//!
//! Contains the PermissionChecker trait, RbacService implementation, and mock implementation.

pub mod mock;
mod permission_checker;
pub mod service;

pub use mock::MockPermissionChecker;
pub use permission_checker::PermissionChecker;
pub use service::RbacService;
