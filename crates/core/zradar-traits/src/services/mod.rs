//! Service-layer trait definitions.
//!
//! These traits define the business-logic boundary between transport layers
//! (HTTP/gRPC) and the underlying storage/policy implementations.

pub mod audit;
pub mod policy;
pub mod query;
pub mod retention;
pub mod settings;

pub use audit::AuditQueryService;
pub use policy::PolicyAdminService;
pub use query::{AnalyticsQueryService, TelemetryQueryService};
pub use retention::RetentionService;
pub use settings::SettingsAdminService;
