//! Retention configuration store.
//!
//! `RetentionConfigStore` holds per-workspace retention rules
//! in memory (backed by `DashMap`) so every query path can compute the
//! effective retention window without touching the database.
//!
//! Configuration is populated either from the server's static config
//! (`ParquetStorageConfig::retention_days` as the global default) or from
//! a platform config push (Phase 04). Until Phase 04 lands, calling code sets
//! the global default and leaves workspace-level overrides empty.

use dashmap::DashMap;
use zradar_models::WorkspaceId;

/// Retention configuration for a single workspace.
#[derive(Debug, Clone)]
pub struct WorkspaceRetentionConfig {
    pub workspace_id: WorkspaceId,
    /// Override retention in days for this workspace.
    pub retention_days: u32,
}

/// Thread-safe, in-memory store for retention configuration.
///
/// Lookups fall back: workspace override → global default.
pub struct RetentionConfigStore {
    configs: DashMap<WorkspaceId, WorkspaceRetentionConfig>,
    /// Fallback retention days used when no workspace-level config exists.
    global_default_days: u32,
}

impl RetentionConfigStore {
    /// Create a new store with the given global default.
    pub fn new(global_default_days: u32) -> Self {
        Self {
            configs: DashMap::new(),
            global_default_days,
        }
    }

    /// Insert or replace the retention config for a workspace.
    pub fn upsert(&self, config: WorkspaceRetentionConfig) {
        self.configs.insert(config.workspace_id, config);
    }

    /// Insert or update a workspace-level retention override.
    pub fn upsert_workspace_override(&self, workspace_id: WorkspaceId, days: u32) {
        self.configs.insert(
            workspace_id,
            WorkspaceRetentionConfig {
                workspace_id,
                retention_days: days,
            },
        );
    }

    /// Get the effective retention in days for a workspace.
    ///
    /// Lookup order:
    /// 1. Workspace override
    /// 2. Global default
    pub fn get_effective_days(&self, workspace_id: WorkspaceId) -> u32 {
        if let Some(cfg) = self.configs.get(&workspace_id) {
            return cfg.retention_days;
        }
        self.global_default_days
    }

    /// Compute the retention cutoff as microseconds since epoch.
    ///
    /// Data with `max_ts < cutoff` is eligible for deletion.
    pub fn get_cutoff_us(&self, workspace_id: WorkspaceId) -> i64 {
        let days = self.get_effective_days(workspace_id);
        let secs = (days as i64) * 86_400;
        chrono::Utc::now().timestamp_micros() - secs * 1_000_000
    }

    /// Compute the retention cutoff as nanoseconds since epoch.
    pub fn get_cutoff_ns(&self, workspace_id: WorkspaceId) -> i64 {
        let days = self.get_effective_days(workspace_id);
        let secs = (days as i64) * 86_400;
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - secs * 1_000_000_000
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;

    fn make_store() -> RetentionConfigStore {
        RetentionConfigStore::new(30)
    }

    #[test]
    fn test_global_default_when_no_workspace_config() {
        let store = make_store();
        let days = store.get_effective_days(Uuid::new_v4().into());
        assert_eq!(days, 30);
    }

    #[test]
    fn test_workspace_override_wins() {
        let store = make_store();
        let workspace_id = Uuid::new_v4();
        store.upsert(WorkspaceRetentionConfig {
            workspace_id: workspace_id.into(),
            retention_days: 14,
        });
        assert_eq!(store.get_effective_days(workspace_id.into()), 14);
        // Other workspace still uses global default
        assert_eq!(store.get_effective_days(Uuid::new_v4().into()), 30);
    }

    #[test]
    fn test_cutoff_us_zero_days_is_approximately_now() {
        let store = RetentionConfigStore::new(0);
        let cutoff = store.get_cutoff_us(Uuid::new_v4().into());
        let now = chrono::Utc::now().timestamp_micros();
        // Allow 1-second tolerance
        assert!((now - cutoff).abs() < 1_000_000);
    }
}
