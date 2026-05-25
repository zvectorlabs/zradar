//! HTTP layer — router and auth extractor.

pub mod auth_extractor;
pub mod router;

pub use auth_extractor::{AuthContext, AuthMode};
pub use router::create_admin_router;
