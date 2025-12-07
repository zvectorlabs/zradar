//! Project management domain
//!
//! This module contains all project-related functionality:
//! - Types and DTOs
//! - Business logic service
//! - HTTP handlers
//! - Router

pub mod handlers;
pub mod router;
pub mod service;
pub mod types;

// Re-export for convenience
pub use service::ProjectService;
pub use types::*;
