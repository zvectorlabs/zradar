//! Audit logging shared module - re-exports from zradar_traits

pub mod mock;

// Re-export from traits
pub use zradar_traits::{AuditLogger, AuditLog, AuditEvent, AuditStatus};
pub use mock::MockAuditLogger;
