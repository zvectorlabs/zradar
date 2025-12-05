//! roles management domain
//!
//! This module contains all roles-related functionality:
//! - Types and DTOs
//! - Business logic service
//! - HTTP handlers

pub mod types;
pub mod service;
pub mod handlers;
pub mod router;

// Re-export for convenience
pub use types::*;
pub use service::RoleService;
