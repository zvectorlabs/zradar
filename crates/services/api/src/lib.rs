//! # api
//!
//! REST API for zradar — telemetry queries and retention management.

/// Audit log admin API
pub mod audit;

/// Telemetry query domain
pub mod telemetry;

/// Retention admin API
pub mod retention;

/// Project settings API
pub mod settings;

/// Policy admin API
pub mod policy;

/// HTTP layer
pub mod http;

/// gRPC transport layer
pub mod grpc;

/// Error types
pub mod errors;

// Re-exports
pub use errors::{ControlError, Result};
pub use zradar_traits::{TelemetryReader, TelemetryWriter};
