//! HTTP layer - router and utilities
//!
//! This module provides HTTP routing infrastructure.
//! Handlers are now in their respective domain modules.

pub mod extractors;
pub mod router;

// Re-export main types
pub use router::{create_admin_router, ApiDoc};
pub use extractors::AuthenticatedUser;
