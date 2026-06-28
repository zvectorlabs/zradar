use async_trait::async_trait;
use zradar_models::WorkspaceId;

use crate::errors::PolicyError;
use crate::types::{
    Decision, DecisionAuditEvent, IngestCtx, IngestRateRecord, Operation, Policy, PolicyId,
    QueryCtx, QuerySample, QueryUsageRecord, RateSample, ResolvedPolicy, RetentionUsageBucket,
    SignalKind, ThresholdEvent, UsageDailyRecord, WriteSample,
};

#[async_trait]
pub trait PolicyStore: Send + Sync {
    async fn upsert(&self, policy: Policy) -> Result<(), PolicyError>;

    /// Upsert multiple policies as one logical operation.
    ///
    /// The default implementation intentionally delegates to `upsert` in a loop so existing
    /// in-memory, noop, and test stores keep working without needing a bulk-specific
    /// implementation. Stores with transactional backing storage should override this to apply
    /// the full batch atomically and avoid repeated cache refreshes.
    async fn upsert_many(&self, policies: Vec<Policy>) -> Result<(), PolicyError> {
        for policy in policies {
            self.upsert(policy).await?;
        }
        Ok(())
    }
    async fn delete(&self, id: PolicyId) -> Result<(), PolicyError>;
    async fn list(&self, workspace_id: WorkspaceId) -> Result<Vec<Policy>, PolicyError>;

    fn resolve(
        &self,
        workspace_id: WorkspaceId,
        signal: SignalKind,
        operation: Operation,
    ) -> ResolvedPolicy;
}

#[async_trait]
pub trait UsageReader: Send + Sync {
    async fn current_rate(
        &self,
        workspace_id: WorkspaceId,
        signal: SignalKind,
        operation: Operation,
    ) -> Result<RateSample, PolicyError>;

    async fn period_used_bytes(
        &self,
        workspace_id: WorkspaceId,
        signal: SignalKind,
        operation: Operation,
        period_start: i64,
        period_end: Option<i64>,
    ) -> Result<i64, PolicyError>;

    async fn stored_compressed_bytes(
        &self,
        workspace_id: WorkspaceId,
        signal: SignalKind,
    ) -> Result<i64, PolicyError>;

    async fn retention_buckets(
        &self,
        workspace_id: WorkspaceId,
        signal: SignalKind,
    ) -> Result<Vec<RetentionUsageBucket>, PolicyError>;
}

#[async_trait]
pub trait UsageTracker: Send + Sync {
    async fn record_write(&self, sample: WriteSample);
    async fn record_query(&self, sample: QuerySample);
}

#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn check_ingest(&self, ctx: IngestCtx) -> Decision;
    async fn check_query(&self, ctx: QueryCtx) -> Decision;
}

#[async_trait]
pub trait ThresholdSink: Send + Sync {
    async fn emit(&self, event: ThresholdEvent) -> Result<(), PolicyError>;
}

#[async_trait]
pub trait DecisionAuditSink: Send + Sync {
    async fn record(&self, event: DecisionAuditEvent) -> Result<(), PolicyError>;
}

#[async_trait]
pub trait UsageAnalyticsReader: Send + Sync {
    async fn usage_daily(
        &self,
        workspace_id: WorkspaceId,
        signal: Option<SignalKind>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> Result<Vec<UsageDailyRecord>, PolicyError>;

    async fn ingest_rate(
        &self,
        workspace_id: WorkspaceId,
        signal: Option<SignalKind>,
        window_start_micros: i64,
        window_end_micros: i64,
    ) -> Result<Vec<IngestRateRecord>, PolicyError>;

    async fn query_usage(
        &self,
        workspace_id: WorkspaceId,
        signal: Option<SignalKind>,
        window_start_micros: i64,
        window_end_micros: i64,
    ) -> Result<Vec<QueryUsageRecord>, PolicyError>;
}

#[derive(Debug, Default)]
pub struct EmptyPolicyStore;

#[async_trait]
impl PolicyStore for EmptyPolicyStore {
    async fn upsert(&self, _policy: Policy) -> Result<(), PolicyError> {
        Ok(())
    }

    async fn delete(&self, _id: PolicyId) -> Result<(), PolicyError> {
        Ok(())
    }

    async fn list(&self, _workspace_id: WorkspaceId) -> Result<Vec<Policy>, PolicyError> {
        Ok(Vec::new())
    }

    fn resolve(
        &self,
        _workspace_id: WorkspaceId,
        _signal: SignalKind,
        _operation: Operation,
    ) -> ResolvedPolicy {
        ResolvedPolicy::default()
    }
}

#[derive(Debug, Default)]
pub struct EmptyUsageReader;

#[async_trait]
impl UsageReader for EmptyUsageReader {
    async fn current_rate(
        &self,
        _workspace_id: WorkspaceId,
        _signal: SignalKind,
        _operation: Operation,
    ) -> Result<RateSample, PolicyError> {
        Ok(RateSample {
            records_per_sec: 0,
            bytes_per_sec: 0,
            sampled_at_micros: 0,
        })
    }

    async fn period_used_bytes(
        &self,
        _workspace_id: WorkspaceId,
        _signal: SignalKind,
        _operation: Operation,
        _period_start: i64,
        _period_end: Option<i64>,
    ) -> Result<i64, PolicyError> {
        Ok(0)
    }

    async fn stored_compressed_bytes(
        &self,
        _workspace_id: WorkspaceId,
        _signal: SignalKind,
    ) -> Result<i64, PolicyError> {
        Ok(0)
    }

    async fn retention_buckets(
        &self,
        _workspace_id: WorkspaceId,
        _signal: SignalKind,
    ) -> Result<Vec<RetentionUsageBucket>, PolicyError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
pub struct NoopUsageTracker;

#[async_trait]
impl UsageTracker for NoopUsageTracker {
    async fn record_write(&self, _sample: WriteSample) {}
    async fn record_query(&self, _sample: QuerySample) {}
}

#[derive(Debug, Default)]
pub struct AllowAllPolicyEnforcer;

#[async_trait]
impl PolicyEnforcer for AllowAllPolicyEnforcer {
    async fn check_ingest(&self, _ctx: IngestCtx) -> Decision {
        Decision::Allow
    }

    async fn check_query(&self, _ctx: QueryCtx) -> Decision {
        Decision::Allow
    }
}

#[derive(Debug, Default)]
pub struct NoopThresholdSink;

#[async_trait]
impl ThresholdSink for NoopThresholdSink {
    async fn emit(&self, _event: ThresholdEvent) -> Result<(), PolicyError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoopDecisionAuditSink;

#[async_trait]
impl DecisionAuditSink for NoopDecisionAuditSink {
    async fn record(&self, _event: DecisionAuditEvent) -> Result<(), PolicyError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::types::{PolicyLimit, PolicySource, UsageBasis};

    use super::*;

    struct CountingPolicyStore {
        upsert_count: AtomicUsize,
    }

    #[async_trait]
    impl PolicyStore for CountingPolicyStore {
        async fn upsert(&self, _policy: Policy) -> Result<(), PolicyError> {
            self.upsert_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn delete(&self, _id: PolicyId) -> Result<(), PolicyError> {
            Ok(())
        }

        async fn list(&self, _workspace_id: WorkspaceId) -> Result<Vec<Policy>, PolicyError> {
            Ok(Vec::new())
        }

        fn resolve(
            &self,
            _workspace_id: WorkspaceId,
            _signal: SignalKind,
            _operation: Operation,
        ) -> ResolvedPolicy {
            ResolvedPolicy::default()
        }
    }

    fn policy(workspace_id: WorkspaceId, max_bytes: i64) -> Policy {
        Policy {
            id: None,
            workspace_id,
            signal: SignalKind::Traces,
            operation: Operation::Ingest,
            limit: PolicyLimit::Size {
                max_bytes,
                basis: UsageBasis::CompressedBytes,
            },
            grace_pct: 101,
            hard_block_pct: 103,
            effective_from: 0,
            effective_until: None,
            source: PolicySource::Api,
        }
    }

    #[tokio::test]
    async fn upsert_many_default_delegates_to_upsert_for_each_policy() {
        let store = CountingPolicyStore {
            upsert_count: AtomicUsize::new(0),
        };
        let workspace_id = Uuid::new_v4();

        store
            .upsert_many(vec![
                policy(workspace_id.into(), 10),
                policy(workspace_id.into(), 20),
            ])
            .await
            .unwrap();

        assert_eq!(store.upsert_count.load(Ordering::Relaxed), 2);
    }
}
