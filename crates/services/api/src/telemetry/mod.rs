//! telemetry management domain
//!
//! This module contains all telemetry-related functionality:
//! - Types and DTOs
//! - Business logic service
//! - HTTP handlers

pub mod handlers;
pub mod router;
pub mod service;
pub mod types;

// Re-export for convenience
pub use service::QueryService;
pub use types::*;
