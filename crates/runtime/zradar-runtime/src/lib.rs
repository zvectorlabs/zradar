//! zradar server runtime.
//!
//! This crate owns server startup wiring: PostgreSQL, Parquet storage, WAL,
//! retention, OTLP gRPC, and Admin HTTP. Callers supply authentication and
//! admin-context strategies via [`RuntimeAuth`].
//!
//! OSS `zradar-server` supplies [`zradar_auth_config::ConfigAuthenticator`] and
//! a config-key `AdminAuthorizer`. External platform wrapper binaries supply
//! their own `Authenticator` and `AdminAuthorizer` implementations.

pub mod admin_key;
pub mod builder;
pub(crate) mod health;

pub use admin_key::ApiKeyAdminAuthorizer;
pub use builder::{RuntimeAuth, ZradarRuntimeBuilder};
pub use zradar_traits::{AdminAuth, AdminAuthorizer, Authenticator};
