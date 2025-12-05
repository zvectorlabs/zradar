//! RBAC shared module
//!
//! Contains the PermissionChecker trait, RbacService implementation, and mock implementation.

mod permission_checker;
pub mod service;
pub mod mock;

pub use permission_checker::PermissionChecker;
pub use service::RbacService;
pub use mock::MockPermissionChecker;

