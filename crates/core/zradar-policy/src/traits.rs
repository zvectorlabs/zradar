use async_trait::async_trait;
use uuid::Uuid;

use crate::errors::PolicyError;
use crate::types::{
    Decision, DecisionAuditEvent, IngestCtx, IngestRateRecord, Operation, Policy, PolicyId,
    QueryCtx, QuerySample, QueryUsageRecord, RateSample, ResolvedPolicy, RetentionUsageBucket,
    SignalKind, ThresholdEvent, UsageDailyRecord, WriteSample,
};

#[async_trait]
pub trait PolicyStore: Send + Sync {
    async fn upsert(&self, policy: Policy) -> Result<(), PolicyError>;
    async fn delete(&self, id: PolicyId) -> Result<(), PolicyError>;
    async fn list(&self, tenant_id: Uuid) -> Result<Vec<Policy>, PolicyError>;

    fn resolve(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
    ) -> ResolvedPolicy;
}

#[async_trait]
pub trait UsageReader: Send + Sync {
    async fn current_rate(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
    ) -> Result<RateSample, PolicyError>;

    async fn period_used_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
        period_start: i64,
        period_end: Option<i64>,
    ) -> Result<i64, PolicyError>;

    async fn stored_compressed_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
    ) -> Result<i64, PolicyError>;

    async fn retention_buckets(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
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
        tenant_id: Uuid,
        project_id: Uuid,
        signal: Option<SignalKind>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> Result<Vec<UsageDailyRecord>, PolicyError>;

    async fn ingest_rate(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: Option<SignalKind>,
        window_start_micros: i64,
        window_end_micros: i64,
    ) -> Result<Vec<IngestRateRecord>, PolicyError>;

    async fn query_usage(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
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

    async fn list(&self, _tenant_id: Uuid) -> Result<Vec<Policy>, PolicyError> {
        Ok(Vec::new())
    }

    fn resolve(
        &self,
        _tenant_id: Uuid,
        _project_id: Uuid,
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
        _tenant_id: Uuid,
        _project_id: Uuid,
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
        _tenant_id: Uuid,
        _project_id: Uuid,
        _signal: SignalKind,
        _operation: Operation,
        _period_start: i64,
        _period_end: Option<i64>,
    ) -> Result<i64, PolicyError> {
        Ok(0)
    }

    async fn stored_compressed_bytes(
        &self,
        _tenant_id: Uuid,
        _project_id: Uuid,
        _signal: SignalKind,
    ) -> Result<i64, PolicyError> {
        Ok(0)
    }

    async fn retention_buckets(
        &self,
        _tenant_id: Uuid,
        _project_id: Uuid,
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
