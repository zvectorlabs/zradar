//! scores management domain
//!
//! This module contains all scores-related functionality:
//! - Types and DTOs
//! - Business logic service
//! - HTTP handlers

pub mod handlers;
pub mod router;
pub mod service;
pub mod types;

// Re-export for convenience
pub use service::ScoresService;
pub use types::*;
