//! QueryEnforcer — enforces retention limits at query time.
//!
//! When a query's `start_time` falls before the effective retention cutoff the
//! enforcer either:
//! - **Clamps** the start to the cutoff (default, user-friendly), or
//! - **Rejects** the query with an error (strict compliance).
//!
//! The enforcer is injected into `QueryService` as `Option<Arc<QueryEnforcer>>`
//! so it is a no-op when retention is not configured.

use std::sync::Arc;
use zradar_models::WorkspaceId;

use crate::config::RetentionConfigStore;

/// How the enforcer handles a query that exceeds the retention window.
#[derive(Debug, Clone, Copy, Default)]
pub enum EnforcementStrategy {
    /// Adjust the query's start time to the retention cutoff.
    #[default]
    Clamp,
    /// Return an error if the query start is before the cutoff.
    Reject,
}

/// Result of applying the enforcer to a single time-range query.
#[derive(Debug, Clone)]
pub struct EnforcementResult {
    /// Whether the start time was modified.
    pub modified: bool,
    /// Effective retention in days used for this decision.
    pub retention_days: u32,
    /// The start time that will actually be used (nanoseconds).
    pub effective_start_ns: i64,
}

/// Enforces retention limits on telemetry queries.
pub struct QueryEnforcer {
    config_store: Arc<RetentionConfigStore>,
    strategy: EnforcementStrategy,
}

impl QueryEnforcer {
    /// Create a new enforcer with the given strategy.
    pub fn new(config_store: Arc<RetentionConfigStore>, strategy: EnforcementStrategy) -> Self {
        Self {
            config_store,
            strategy,
        }
    }

    /// Enforce retention on a query's start time (nanoseconds).
    ///
    /// Returns `(effective_start_ns, EnforcementResult)`.
    ///
    /// If `start_ns` is `None` the cutoff is used as the start — this prevents
    /// unbounded scans on workspaces with a configured retention window.
    pub fn enforce(
        &self,
        workspace_id: WorkspaceId,
        start_ns: Option<i64>,
    ) -> Result<(i64, EnforcementResult), anyhow::Error> {
        let retention_days = self.config_store.get_effective_days(workspace_id);

        let cutoff_ns = self.config_store.get_cutoff_ns(workspace_id);

        let (effective_start, modified) = match start_ns {
            None => (cutoff_ns, false),
            Some(s) if s < cutoff_ns => match self.strategy {
                EnforcementStrategy::Clamp => (cutoff_ns, true),
                EnforcementStrategy::Reject => {
                    return Err(anyhow::anyhow!(
                        "Query start time is before the retention cutoff \
                         ({retention_days} days). Earliest allowed start is approximately \
                         {retention_days} days ago."
                    ));
                }
            },
            Some(s) => (s, false),
        };

        Ok((
            effective_start,
            EnforcementResult {
                modified,
                retention_days,
                effective_start_ns: effective_start,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use crate::config::WorkspaceRetentionConfig;

    fn make_enforcer(days: u32) -> QueryEnforcer {
        let store = Arc::new(RetentionConfigStore::new(days));
        QueryEnforcer::new(store, EnforcementStrategy::Clamp)
    }

    #[test]
    fn test_clamp_start_before_cutoff() {
        let enforcer = make_enforcer(7);
        let workspace_id = Uuid::new_v4();

        // Start is 30 days ago — before the 7-day cutoff
        let thirty_days_ago_ns =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 30 * 86_400 * 1_000_000_000;

        let (effective_start, result) = enforcer
            .enforce(workspace_id.into(), Some(thirty_days_ago_ns))
            .unwrap();

        assert!(result.modified, "start should have been clamped");
        assert_eq!(result.retention_days, 7);
        // effective_start should be approximately 7 days ago (within 1 second)
        let seven_days_ago_ns =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 7 * 86_400 * 1_000_000_000;
        assert!((effective_start - seven_days_ago_ns).abs() < 1_000_000_000);
    }

    #[test]
    fn test_no_clamp_when_start_within_retention() {
        let enforcer = make_enforcer(30);
        let workspace_id = Uuid::new_v4();

        let five_days_ago_ns =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 5 * 86_400 * 1_000_000_000;

        let (effective_start, result) = enforcer
            .enforce(workspace_id.into(), Some(five_days_ago_ns))
            .unwrap();

        assert!(!result.modified);
        assert_eq!(effective_start, five_days_ago_ns);
    }

    #[test]
    fn test_reject_strategy_errors_on_old_start() {
        let store = Arc::new(RetentionConfigStore::new(7));
        let enforcer = QueryEnforcer::new(store, EnforcementStrategy::Reject);

        let old_ns =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 30 * 86_400 * 1_000_000_000;

        let result = enforcer.enforce(Uuid::new_v4().into(), Some(old_ns));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("retention cutoff"));
    }

    #[test]
    fn test_none_start_uses_cutoff() {
        let enforcer = make_enforcer(7);
        let (_, result) = enforcer.enforce(Uuid::new_v4().into(), None).unwrap();
        assert!(!result.modified);
        assert_eq!(result.retention_days, 7);
    }

    #[test]
    fn test_workspace_override_used_in_enforcement() {
        let store = Arc::new(RetentionConfigStore::new(30));
        let workspace_id = Uuid::new_v4();

        store.upsert(WorkspaceRetentionConfig {
            workspace_id: workspace_id.into(),
            retention_days: 3,
        });

        let enforcer = QueryEnforcer::new(store, EnforcementStrategy::Clamp);

        // 10 days ago — beyond 3-day workspace retention
        let ten_days_ago =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 10 * 86_400 * 1_000_000_000;

        let (_, result) = enforcer
            .enforce(workspace_id.into(), Some(ten_days_ago))
            .unwrap();

        assert!(result.modified);
        assert_eq!(result.retention_days, 3);
    }
}
