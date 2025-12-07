//! User management and authentication domain
//!
//! This module contains all user-related functionality:
//! - Types and DTOs
//! - Authentication service
//! - HTTP handlers
//! - Router

pub mod handlers;
pub mod router;
pub mod service;
pub mod types;

// Re-export for convenience
pub use service::AuthService;
pub use types::*;
