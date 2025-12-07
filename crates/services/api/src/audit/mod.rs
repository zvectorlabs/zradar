//! Audit logging shared module - re-exports from zradar_traits

pub mod mock;

// Re-export from traits
pub use mock::MockAuditLogger;
pub use zradar_traits::{AuditEvent, AuditLog, AuditLogger, AuditStatus};
