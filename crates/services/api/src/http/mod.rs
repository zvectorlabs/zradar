//! HTTP layer — router and auth context extractor.

pub mod auth_extractor;
pub mod router;

pub use auth_extractor::{AuthContext, AuthMode, Capability, parse_ctx_uuid};
pub use router::create_admin_router;
