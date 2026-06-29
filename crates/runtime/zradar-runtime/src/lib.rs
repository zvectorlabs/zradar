//! zradar server runtime.
//!
//! This crate owns server startup wiring: PostgreSQL, Parquet storage, WAL,
//! retention, OTLP gRPC, and Admin HTTP. Callers supply authentication and
//! admin-context strategies via [`RuntimeAuth`].
//!
//! OSS `zradar-server` supplies [`zradar_auth_config::ConfigAuthenticator`] and
//! config-key query/admin authorizers. External platform wrapper binaries supply
//! their own `Authenticator`, `QueryAuthorizer`, and `AdminAuthorizer` implementations.

pub mod builder;
pub mod config_key;
pub(crate) mod health;

pub use builder::{RuntimeAuth, ZradarRuntimeBuilder};
pub use config_key::{
    ApiKeyAdminAuthorizer, ApiKeyQueryAuthorizer, api_key_authorizers_from_config,
};
pub use zradar_traits::{AdminAuth, AdminAuthorizer, Authenticator, QueryAuth, QueryAuthorizer};
