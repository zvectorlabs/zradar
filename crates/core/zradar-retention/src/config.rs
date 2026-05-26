//! Retention configuration store.
//!
//! `RetentionConfigStore` holds per-organisation, per-project retention rules
//! in memory (backed by `DashMap`) so every query path can compute the
//! effective retention window without touching the database.
//!
//! Configuration is populated either from the server's static config
//! (`ParquetStorageConfig::retention_days` as the global default) or from
//! a platform config push (Phase 04). Until Phase 04 lands, calling code sets
//! the global default and leaves org-level overrides empty.

use std::collections::HashMap;

use dashmap::DashMap;
use uuid::Uuid;

/// Per-project retention in days.
pub type ProjectRetentionMap = HashMap<Uuid, u32>;

/// Retention configuration for a single organisation.
#[derive(Debug, Clone)]
pub struct OrgRetentionConfig {
    pub org_id: Uuid,
    /// Default retention in days for all projects in this org.
    pub default_days: u32,
    /// Per-project overrides.  Project-level value wins over `default_days`.
    pub project_overrides: ProjectRetentionMap,
}

/// Thread-safe, in-memory store for retention configuration.
///
/// Lookups fall back: project override → org default → global default.
pub struct RetentionConfigStore {
    configs: DashMap<Uuid, OrgRetentionConfig>,
    /// Fallback retention days used when no org-level config exists.
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

    /// Insert or replace the retention config for an org.
    pub fn upsert(&self, config: OrgRetentionConfig) {
        self.configs.insert(config.org_id, config);
    }

    /// Get the effective retention in days for a project.
    ///
    /// Lookup order:
    /// 1. Project override in org config
    /// 2. Org default
    /// 3. Global default
    pub fn get_effective_days(&self, org_id: Uuid, project_id: Uuid) -> u32 {
        if let Some(cfg) = self.configs.get(&org_id) {
            if let Some(&days) = cfg.project_overrides.get(&project_id) {
                return days;
            }
            return cfg.default_days;
        }
        self.global_default_days
    }

    /// Compute the retention cutoff as microseconds since epoch.
    ///
    /// Data with `max_ts < cutoff` is eligible for deletion.
    pub fn get_cutoff_us(&self, org_id: Uuid, project_id: Uuid) -> i64 {
        let days = self.get_effective_days(org_id, project_id);
        let secs = (days as i64) * 86_400;
        chrono::Utc::now().timestamp_micros() - secs * 1_000_000
    }

    /// Compute the retention cutoff as nanoseconds since epoch.
    pub fn get_cutoff_ns(&self, org_id: Uuid, project_id: Uuid) -> i64 {
        let days = self.get_effective_days(org_id, project_id);
        let secs = (days as i64) * 86_400;
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - secs * 1_000_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> RetentionConfigStore {
        RetentionConfigStore::new(30)
    }

    #[test]
    fn test_global_default_when_no_org_config() {
        let store = make_store();
        let days = store.get_effective_days(Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(days, 30);
    }

    #[test]
    fn test_org_default_overrides_global() {
        let store = make_store();
        let org_id = Uuid::new_v4();
        store.upsert(OrgRetentionConfig {
            org_id,
            default_days: 7,
            project_overrides: HashMap::new(),
        });
        let days = store.get_effective_days(org_id, Uuid::new_v4());
        assert_eq!(days, 7);
    }

    #[test]
    fn test_project_override_wins_over_org_default() {
        let store = make_store();
        let org_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        store.upsert(OrgRetentionConfig {
            org_id,
            default_days: 14,
            project_overrides: [(project_id, 3)].into(),
        });
        assert_eq!(store.get_effective_days(org_id, project_id), 3);
        // Other project still uses org default
        assert_eq!(store.get_effective_days(org_id, Uuid::new_v4()), 14);
    }

    #[test]
    fn test_cutoff_us_zero_days_is_approximately_now() {
        let store = RetentionConfigStore::new(0);
        let cutoff = store.get_cutoff_us(Uuid::new_v4(), Uuid::new_v4());
        let now = chrono::Utc::now().timestamp_micros();
        // Allow 1-second tolerance
        assert!((now - cutoff).abs() < 1_000_000);
    }
}
