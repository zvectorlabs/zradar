//! HTTP layer - router and utilities
//!
//! This module provides HTTP routing infrastructure.
//! Handlers are now in their respective domain modules.

pub mod extractors;
pub mod router;

// Re-export main types
pub use extractors::AuthenticatedUser;
pub use router::{ApiDoc, create_admin_router};
