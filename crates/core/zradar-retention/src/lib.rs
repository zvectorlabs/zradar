//! # zradar-retention
//!
//! Retention system for zradar telemetry data.
//!
//! ## Modules
//!
//! - `config` — `RetentionConfigStore`: per-org, per-project retention rules
//! - `cleanup` — `CleanupJob`: marks expired files `deleted=true` (policy only)
//! - `file_reclaimer` — `FileReclaimer`: lease-aware physical deletion chokepoint
//! - `enforcer` — `QueryEnforcer`: clamps or rejects queries beyond retention window

pub mod cleanup;
pub mod config;
pub mod enforcer;
pub mod file_reclaimer;
pub mod storage_usage_daily;

pub use cleanup::{CleanupJob, CleanupStats, RetentionRunStats};
pub use config::{RetentionConfigStore, WorkspaceRetentionConfig};
pub use enforcer::{EnforcementResult, EnforcementStrategy, QueryEnforcer};
pub use file_reclaimer::{FileReclaimer, ReclaimStats};
pub use storage_usage_daily::StorageUsageDailyJob;
