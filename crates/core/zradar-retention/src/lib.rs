//! # zradar-retention
//!
//! Retention system for zradar telemetry data.
//!
//! ## Modules
//!
//! - `config` — `RetentionConfigStore`: per-org, per-project retention rules
//! - `cleanup` — `CleanupJob`: deletes expired Parquet files on a schedule
//! - `enforcer` — `QueryEnforcer`: clamps or rejects queries beyond retention window

pub mod cleanup;
pub mod config;
pub mod enforcer;
pub mod storage_usage_daily;

pub use cleanup::{CleanupJob, CleanupStats};
pub use config::{OrgRetentionConfig, RetentionConfigStore};
pub use enforcer::{EnforcementResult, EnforcementStrategy, QueryEnforcer};
pub use storage_usage_daily::StorageUsageDailyJob;
