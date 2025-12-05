//! User management and authentication domain
//!
//! This module contains all user-related functionality:
//! - Types and DTOs
//! - Authentication service
//! - HTTP handlers
//! - Router

pub mod types;
pub mod service;
pub mod handlers;
pub mod router;

// Re-export for convenience
pub use types::*;
pub use service::AuthService;

