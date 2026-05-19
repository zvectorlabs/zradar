//! Retention admin API — trigger cleanup and manage retention config.

pub mod handlers;
pub mod router;

pub use router::retention_router;
