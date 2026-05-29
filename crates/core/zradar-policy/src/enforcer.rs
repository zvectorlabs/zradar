use std::sync::Arc;

use async_trait::async_trait;

use crate::traits::{
    DecisionAuditSink, NoopDecisionAuditSink, PolicyEnforcer, PolicyStore, ThresholdSink,
    UsageReader,
};
use crate::types::{
    BlockCode, Decision, DecisionAuditEvent, DecisionSummary, IngestCtx, Operation, PolicyLimit,
    QueryCtx, ThresholdEvent, UsageBasis,
};

const DAY_MICROS: i64 = 86_400 * 1_000_000;

pub struct DefaultPolicyEnforcer {
    store: Arc<dyn PolicyStore>,
    usage: Arc<dyn UsageReader>,
    threshold_sink: Arc<dyn ThresholdSink>,
    decision_audit_sink: Arc<dyn DecisionAuditSink>,
}

impl DefaultPolicyEnforcer {
    pub fn new(
        store: Arc<dyn PolicyStore>,
        usage: Arc<dyn UsageReader>,
        threshold_sink: Arc<dyn ThresholdSink>,
    ) -> Self {
        let decision_audit_sink: Arc<dyn DecisionAuditSink> = Arc::new(NoopDecisionAuditSink);
        Self {
            store,
            usage,
            threshold_sink,
            decision_audit_sink,
        }
    }

    pub fn with_decision_audit_sink(mut self, sink: Arc<dyn DecisionAuditSink>) -> Self {
        self.decision_audit_sink = sink;
        self
    }

    async fn audit_decision(&self, ctx: AuditContext, decision: &Decision) {
        if matches!(decision, Decision::Allow) {
            return;
        }

        let _ = self
            .decision_audit_sink
            .record(DecisionAuditEvent {
                tenant_id: ctx.tenant_id,
                project_id: ctx.project_id,
                signal: ctx.signal,
                operation: ctx.operation,
                decision: DecisionSummary::from(decision),
                reason: decision_reason(decision).to_string(),
                observed_value: None,
                limit_value: None,
                block_code: decision_block_code(decision),
                created_at: ctx.created_at,
            })
            .await;
    }
}

#[async_trait]
impl PolicyEnforcer for DefaultPolicyEnforcer {
    async fn check_ingest(&self, ctx: IngestCtx) -> Decision {
        let resolved =
            self.store
                .resolve(ctx.tenant_id, ctx.project_id, ctx.signal, Operation::Ingest);

        if resolved.blocked {
            let decision = Decision::Block {
                reason: "project_blocked",
                code: BlockCode::ProjectBlocked,
            };
            self.audit_decision(
                AuditContext::from_ingest(&ctx, Operation::Ingest),
                &decision,
            )
            .await;
            return decision;
        }

        if let Some(decision) = check_rate(
            &*self.usage,
            &*self.threshold_sink,
            &ctx,
            &resolved.rate,
            Operation::Ingest,
            resolved.grace_pct,
            resolved.hard_block_pct,
        )
        .await
        {
            self.audit_decision(
                AuditContext::from_ingest(&ctx, Operation::Ingest),
                &decision,
            )
            .await;
            return decision;
        }

        for quota in &resolved.quotas {
            if let Some(decision) = check_quota(
                &*self.usage,
                &*self.threshold_sink,
                &ctx,
                quota,
                Operation::Ingest,
                resolved.grace_pct,
                resolved.hard_block_pct,
            )
            .await
            {
                self.audit_decision(
                    AuditContext::from_ingest(&ctx, Operation::Ingest),
                    &decision,
                )
                .await;
                return decision;
            }
        }

        if let Some(decision) = check_size(
            &*self.usage,
            &*self.threshold_sink,
            &ctx,
            &resolved.size,
            resolved.grace_pct,
            resolved.hard_block_pct,
        )
        .await
        {
            self.audit_decision(AuditContext::from_ingest(&ctx, Operation::Store), &decision)
                .await;
            return decision;
        }

        Decision::Allow
    }

    async fn check_query(&self, ctx: QueryCtx) -> Decision {
        let resolved =
            self.store
                .resolve(ctx.tenant_id, ctx.project_id, ctx.signal, Operation::Query);

        if let Some(decision) = check_query_range(&ctx, &resolved.retention, &resolved.query_window)
        {
            self.audit_decision(AuditContext::from_query(&ctx, Operation::Query), &decision)
                .await;
            return decision;
        }

        if let Some(decision) = check_query_rate(
            &*self.usage,
            &*self.threshold_sink,
            &ctx,
            &resolved.rate,
            resolved.grace_pct,
            resolved.hard_block_pct,
        )
        .await
        {
            self.audit_decision(AuditContext::from_query(&ctx, Operation::Query), &decision)
                .await;
            return decision;
        }

        for quota in &resolved.quotas {
            if let Some(decision) = check_query_quota(
                &*self.usage,
                &*self.threshold_sink,
                &ctx,
                quota,
                resolved.grace_pct,
                resolved.hard_block_pct,
            )
            .await
            {
                self.audit_decision(AuditContext::from_query(&ctx, Operation::Query), &decision)
                    .await;
                return decision;
            }
        }

        Decision::Allow
    }
}

async fn check_rate(
    usage: &dyn UsageReader,
    threshold_sink: &dyn ThresholdSink,
    ctx: &IngestCtx,
    rate: &Option<PolicyLimit>,
    operation: Operation,
    grace_pct: u8,
    hard_block_pct: u8,
) -> Option<Decision> {
    let Some(PolicyLimit::Rate {
        records_per_sec,
        bytes_per_sec,
    }) = rate
    else {
        return None;
    };

    let sample = usage
        .current_rate(ctx.tenant_id, ctx.project_id, ctx.signal, operation)
        .await
        .ok()?;

    if let Some(limit) = *records_per_sec {
        let observed = sample.records_per_sec.saturating_add(ctx.records);
        emit_thresholds(
            threshold_sink,
            ThresholdContext::from_ingest(ctx, operation, "rate_records"),
            u64_to_i64_saturating(observed),
            u64_to_i64_saturating(limit),
            None,
            hard_block_pct,
        )
        .await;
        if let Some(decision) = compare_limit(
            observed,
            limit,
            grace_pct,
            hard_block_pct,
            BlockCode::RateLimitExceeded,
            "rate_records_exceeded",
        ) {
            return Some(decision);
        }
    }

    if let (Some(limit), Some(estimated_bytes)) = (*bytes_per_sec, ctx.estimated_bytes) {
        let observed = sample.bytes_per_sec.saturating_add(estimated_bytes);
        emit_thresholds(
            threshold_sink,
            ThresholdContext::from_ingest(ctx, operation, "rate_bytes"),
            u64_to_i64_saturating(observed),
            u64_to_i64_saturating(limit),
            None,
            hard_block_pct,
        )
        .await;
        if let Some(decision) = compare_limit(
            observed,
            limit,
            grace_pct,
            hard_block_pct,
            BlockCode::RateLimitExceeded,
            "rate_bytes_exceeded",
        ) {
            return Some(decision);
        }
    }

    None
}

async fn check_quota(
    usage: &dyn UsageReader,
    threshold_sink: &dyn ThresholdSink,
    ctx: &IngestCtx,
    quota: &PolicyLimit,
    operation: Operation,
    grace_pct: u8,
    hard_block_pct: u8,
) -> Option<Decision> {
    let PolicyLimit::Quota {
        max_bytes,
        period_start,
        period_end,
        basis,
    } = quota
    else {
        return None;
    };

    if *basis != UsageBasis::CompressedBytes {
        return None;
    }

    let used = usage
        .period_used_bytes(
            ctx.tenant_id,
            ctx.project_id,
            ctx.signal,
            operation,
            *period_start,
            *period_end,
        )
        .await
        .ok()?;
    let observed = used.saturating_add(u64_to_i64_saturating(ctx.estimated_bytes.unwrap_or(0)));
    emit_thresholds(
        threshold_sink,
        ThresholdContext::from_ingest(ctx, operation, "quota"),
        observed,
        *max_bytes,
        Some(*period_start),
        hard_block_pct,
    )
    .await;

    compare_limit_i64(
        observed,
        *max_bytes,
        grace_pct,
        hard_block_pct,
        BlockCode::QuotaExceeded,
        "quota_exceeded",
    )
}

async fn check_size(
    usage: &dyn UsageReader,
    threshold_sink: &dyn ThresholdSink,
    ctx: &IngestCtx,
    size: &Option<PolicyLimit>,
    grace_pct: u8,
    hard_block_pct: u8,
) -> Option<Decision> {
    let Some(PolicyLimit::Size { max_bytes, basis }) = size else {
        return None;
    };

    if *basis != UsageBasis::CompressedBytes {
        return None;
    }

    let stored = usage
        .stored_compressed_bytes(ctx.tenant_id, ctx.project_id, ctx.signal)
        .await
        .ok()?;
    emit_thresholds(
        threshold_sink,
        ThresholdContext::from_ingest(ctx, Operation::Store, "size"),
        stored,
        *max_bytes,
        None,
        hard_block_pct,
    )
    .await;

    compare_limit_i64(
        stored,
        *max_bytes,
        grace_pct,
        hard_block_pct,
        BlockCode::SizeExceeded,
        "size_exceeded",
    )
}

fn check_query_range(
    ctx: &QueryCtx,
    retention: &Option<PolicyLimit>,
    query_window: &Option<PolicyLimit>,
) -> Option<Decision> {
    if let Some(PolicyLimit::Retention { max_days }) = retention
        && let Some(start_micros) = ctx.start_micros
    {
        let cutoff = ctx
            .now_micros
            .saturating_sub(i64::from(*max_days).saturating_mul(DAY_MICROS));
        if start_micros < cutoff {
            return Some(Decision::Block {
                reason: "retention_violation",
                code: BlockCode::RetentionViolation,
            });
        }
    }

    if let Some(PolicyLimit::Window { max_query_days }) = query_window
        && let (Some(start_micros), Some(end_micros)) = (ctx.start_micros, ctx.end_micros)
    {
        let max_window_micros = i64::from(*max_query_days).saturating_mul(DAY_MICROS);
        if end_micros.saturating_sub(start_micros) > max_window_micros {
            return Some(Decision::Block {
                reason: "query_window_violation",
                code: BlockCode::QueryWindowViolation,
            });
        }
    }

    None
}

async fn check_query_rate(
    usage: &dyn UsageReader,
    threshold_sink: &dyn ThresholdSink,
    ctx: &QueryCtx,
    rate: &Option<PolicyLimit>,
    grace_pct: u8,
    hard_block_pct: u8,
) -> Option<Decision> {
    let Some(PolicyLimit::Rate { bytes_per_sec, .. }) = rate else {
        return None;
    };

    let limit = (*bytes_per_sec)?;

    let sample = usage
        .current_rate(ctx.tenant_id, ctx.project_id, ctx.signal, Operation::Query)
        .await
        .ok()?;
    let observed = sample
        .bytes_per_sec
        .saturating_add(ctx.estimated_scanned_bytes.unwrap_or(0));
    emit_thresholds(
        threshold_sink,
        ThresholdContext::from_query(ctx, Operation::Query, "query_rate_bytes"),
        u64_to_i64_saturating(observed),
        u64_to_i64_saturating(limit),
        None,
        hard_block_pct,
    )
    .await;

    compare_limit(
        observed,
        limit,
        grace_pct,
        hard_block_pct,
        BlockCode::RateLimitExceeded,
        "query_rate_exceeded",
    )
}

async fn check_query_quota(
    usage: &dyn UsageReader,
    threshold_sink: &dyn ThresholdSink,
    ctx: &QueryCtx,
    quota: &PolicyLimit,
    grace_pct: u8,
    hard_block_pct: u8,
) -> Option<Decision> {
    let PolicyLimit::Quota {
        max_bytes,
        period_start,
        period_end,
        basis,
    } = quota
    else {
        return None;
    };

    if *basis != UsageBasis::ScannedBytes {
        return None;
    }

    let used = usage
        .period_used_bytes(
            ctx.tenant_id,
            ctx.project_id,
            ctx.signal,
            Operation::Query,
            *period_start,
            *period_end,
        )
        .await
        .ok()?;
    let observed = used.saturating_add(u64_to_i64_saturating(
        ctx.estimated_scanned_bytes.unwrap_or(0),
    ));
    emit_thresholds(
        threshold_sink,
        ThresholdContext::from_query(ctx, Operation::Query, "quota"),
        observed,
        *max_bytes,
        Some(*period_start),
        hard_block_pct,
    )
    .await;

    compare_limit_i64(
        observed,
        *max_bytes,
        grace_pct,
        hard_block_pct,
        BlockCode::QuotaExceeded,
        "query_quota_exceeded",
    )
}

fn compare_limit(
    observed: u64,
    limit: u64,
    grace_pct: u8,
    hard_block_pct: u8,
    code: BlockCode,
    reason: &'static str,
) -> Option<Decision> {
    compare_limit_i64(
        u64_to_i64_saturating(observed),
        u64_to_i64_saturating(limit),
        grace_pct,
        hard_block_pct,
        code,
        reason,
    )
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn compare_limit_i64(
    observed: i64,
    limit: i64,
    grace_pct: u8,
    hard_block_pct: u8,
    code: BlockCode,
    reason: &'static str,
) -> Option<Decision> {
    if limit <= 0 {
        return Some(Decision::Block { reason, code });
    }

    if observed.saturating_mul(100) >= limit.saturating_mul(i64::from(hard_block_pct)) {
        return Some(Decision::Block { reason, code });
    }

    if observed.saturating_mul(100) > limit.saturating_mul(i64::from(grace_pct)) {
        return Some(Decision::Throttle {
            retry_after_ms: 1000,
            reason,
        });
    }

    if observed > limit {
        return Some(Decision::AllowWithGrace { reason });
    }

    None
}

struct AuditContext {
    tenant_id: uuid::Uuid,
    project_id: uuid::Uuid,
    signal: crate::types::SignalKind,
    operation: Operation,
    created_at: i64,
}

impl AuditContext {
    fn from_ingest(ctx: &IngestCtx, operation: Operation) -> Self {
        Self {
            tenant_id: ctx.tenant_id,
            project_id: ctx.project_id,
            signal: ctx.signal,
            operation,
            created_at: ctx.now_micros,
        }
    }

    fn from_query(ctx: &QueryCtx, operation: Operation) -> Self {
        Self {
            tenant_id: ctx.tenant_id,
            project_id: ctx.project_id,
            signal: ctx.signal,
            operation,
            created_at: ctx.now_micros,
        }
    }
}

fn decision_reason(decision: &Decision) -> &'static str {
    match decision {
        Decision::Allow => "allow",
        Decision::AllowWithGrace { reason } => reason,
        Decision::Throttle { reason, .. } => reason,
        Decision::Block { reason, .. } => reason,
    }
}

fn decision_block_code(decision: &Decision) -> Option<BlockCode> {
    match decision {
        Decision::Block { code, .. } => Some(*code),
        Decision::Allow | Decision::AllowWithGrace { .. } | Decision::Throttle { .. } => None,
    }
}

struct ThresholdContext<'a> {
    tenant_id: uuid::Uuid,
    project_id: uuid::Uuid,
    signal: crate::types::SignalKind,
    operation: Operation,
    limit_kind: &'a str,
    emitted_at: i64,
}

impl<'a> ThresholdContext<'a> {
    fn from_ingest(ctx: &IngestCtx, operation: Operation, limit_kind: &'a str) -> Self {
        Self {
            tenant_id: ctx.tenant_id,
            project_id: ctx.project_id,
            signal: ctx.signal,
            operation,
            limit_kind,
            emitted_at: ctx.now_micros,
        }
    }

    fn from_query(ctx: &QueryCtx, operation: Operation, limit_kind: &'a str) -> Self {
        Self {
            tenant_id: ctx.tenant_id,
            project_id: ctx.project_id,
            signal: ctx.signal,
            operation,
            limit_kind,
            emitted_at: ctx.now_micros,
        }
    }
}

async fn emit_thresholds(
    threshold_sink: &dyn ThresholdSink,
    ctx: ThresholdContext<'_>,
    observed: i64,
    limit: i64,
    period_start: Option<i64>,
    hard_block_pct: u8,
) {
    if limit <= 0 {
        return;
    }

    let mut thresholds = vec![70_u16, 90, 100, u16::from(hard_block_pct)];
    thresholds.sort_unstable();
    thresholds.dedup();

    for threshold_pct in thresholds {
        if observed.saturating_mul(100) >= limit.saturating_mul(i64::from(threshold_pct)) {
            let _ = threshold_sink
                .emit(ThresholdEvent {
                    tenant_id: ctx.tenant_id,
                    project_id: ctx.project_id,
                    signal: ctx.signal,
                    operation: ctx.operation,
                    limit_kind: ctx.limit_kind.to_string(),
                    threshold_pct,
                    observed_value: observed,
                    limit_value: limit,
                    period_start,
                    emitted_at: ctx.emitted_at,
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{
        DecisionAuditSink, EmptyPolicyStore, EmptyUsageReader, NoopThresholdSink, UsageTracker,
    };
    use crate::types::{
        DecisionAuditEvent, DecisionSummary, Policy, PolicyId, ResolvedPolicy, SignalKind,
        UsageBasis,
    };
    use crate::usage::InMemoryUsageTracker;
    use crate::{PolicyError, QuerySample};
    use std::sync::Mutex;

    struct QueryQuotaPolicyStore {
        max_bytes: i64,
        grace_pct: u8,
        hard_block_pct: u8,
    }

    #[derive(Default)]
    struct CapturingDecisionAuditSink {
        events: Mutex<Vec<DecisionAuditEvent>>,
    }

    #[async_trait::async_trait]
    impl DecisionAuditSink for CapturingDecisionAuditSink {
        async fn record(&self, event: DecisionAuditEvent) -> Result<(), PolicyError> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl PolicyStore for QueryQuotaPolicyStore {
        async fn upsert(&self, _policy: Policy) -> Result<(), PolicyError> {
            Ok(())
        }

        async fn delete(&self, _id: PolicyId) -> Result<(), PolicyError> {
            Ok(())
        }

        async fn list(&self, _tenant_id: uuid::Uuid) -> Result<Vec<Policy>, PolicyError> {
            Ok(Vec::new())
        }

        fn resolve(
            &self,
            _tenant_id: uuid::Uuid,
            _project_id: uuid::Uuid,
            _signal: SignalKind,
            operation: Operation,
        ) -> ResolvedPolicy {
            if operation != Operation::Query {
                return ResolvedPolicy::default();
            }

            ResolvedPolicy {
                quotas: vec![PolicyLimit::Quota {
                    max_bytes: self.max_bytes,
                    period_start: 0,
                    period_end: None,
                    basis: UsageBasis::ScannedBytes,
                }],
                grace_pct: self.grace_pct,
                hard_block_pct: self.hard_block_pct,
                ..ResolvedPolicy::default()
            }
        }
    }

    #[test]
    fn hard_block_uses_configured_percentage() {
        assert_eq!(
            compare_limit_i64(
                103,
                100,
                101,
                103,
                BlockCode::QuotaExceeded,
                "quota_exceeded"
            ),
            Some(Decision::Block {
                reason: "quota_exceeded",
                code: BlockCode::QuotaExceeded,
            })
        );
    }

    #[test]
    fn between_limit_and_grace_allows_with_grace() {
        assert_eq!(
            compare_limit_i64(
                101,
                100,
                102,
                103,
                BlockCode::QuotaExceeded,
                "quota_exceeded"
            ),
            Some(Decision::AllowWithGrace {
                reason: "quota_exceeded"
            })
        );
    }

    #[test]
    fn oversized_u64_limits_fail_closed_instead_of_bypassing() {
        assert_eq!(
            compare_limit(
                u64::MAX,
                1,
                101,
                103,
                BlockCode::RateLimitExceeded,
                "rate_exceeded"
            ),
            Some(Decision::Block {
                reason: "rate_exceeded",
                code: BlockCode::RateLimitExceeded,
            })
        );
    }

    #[test]
    fn huge_day_limits_do_not_overflow_query_range_math() {
        let ctx = QueryCtx {
            tenant_id: uuid::Uuid::new_v4(),
            project_id: uuid::Uuid::new_v4(),
            signal: SignalKind::Traces,
            start_micros: Some(i64::MIN),
            end_micros: Some(i64::MAX),
            estimated_scanned_bytes: None,
            now_micros: i64::MAX,
        };

        assert_eq!(
            check_query_range(
                &ctx,
                &Some(PolicyLimit::Retention { max_days: u32::MAX }),
                &Some(PolicyLimit::Window {
                    max_query_days: u32::MAX,
                }),
            ),
            Some(Decision::Block {
                reason: "retention_violation",
                code: BlockCode::RetentionViolation,
            })
        );
    }

    #[tokio::test]
    async fn empty_policy_allows_ingest() {
        let store: Arc<dyn PolicyStore> = Arc::new(EmptyPolicyStore);
        let usage: Arc<dyn UsageReader> = Arc::new(EmptyUsageReader);
        let threshold_sink: Arc<dyn ThresholdSink> = Arc::new(NoopThresholdSink);
        let enforcer = DefaultPolicyEnforcer::new(store, usage, threshold_sink);
        let decision = enforcer
            .check_ingest(IngestCtx {
                tenant_id: uuid::Uuid::new_v4(),
                project_id: uuid::Uuid::new_v4(),
                signal: SignalKind::Logs,
                records: 1,
                estimated_bytes: None,
                now_micros: 0,
            })
            .await;
        assert_eq!(decision, Decision::Allow);
    }

    #[tokio::test]
    async fn recorded_query_usage_drives_scanned_byte_quota_block() {
        let tenant_id = uuid::Uuid::new_v4();
        let project_id = uuid::Uuid::new_v4();
        let compressed_size = 1_000_i64;
        let quota_limit = compressed_size.saturating_mul(3) / 2;
        let usage = Arc::new(InMemoryUsageTracker::new());
        let store: Arc<dyn PolicyStore> = Arc::new(QueryQuotaPolicyStore {
            max_bytes: quota_limit,
            grace_pct: 101,
            hard_block_pct: 103,
        });
        let threshold_sink: Arc<dyn ThresholdSink> = Arc::new(NoopThresholdSink);
        let usage_reader: Arc<dyn UsageReader> = usage.clone();
        let enforcer = DefaultPolicyEnforcer::new(store, usage_reader, threshold_sink);

        let first_decision = enforcer
            .check_query(QueryCtx {
                tenant_id,
                project_id,
                signal: SignalKind::Traces,
                start_micros: Some(0),
                end_micros: Some(1_000_000),
                estimated_scanned_bytes: Some(compressed_size as u64),
                now_micros: 1,
            })
            .await;
        assert_eq!(first_decision, Decision::Allow);

        usage
            .record_query(QuerySample {
                tenant_id,
                project_id,
                signal: SignalKind::Traces,
                bytes_scanned: compressed_size,
                rows_scanned: Some(1),
                query_time_ms: Some(1),
                decision: DecisionSummary::Allow,
                submitted_at: 1,
            })
            .await;

        let second_decision = enforcer
            .check_query(QueryCtx {
                tenant_id,
                project_id,
                signal: SignalKind::Traces,
                start_micros: Some(0),
                end_micros: Some(1_000_000),
                estimated_scanned_bytes: Some(compressed_size as u64),
                now_micros: 2,
            })
            .await;
        assert_eq!(
            second_decision,
            Decision::Block {
                reason: "query_quota_exceeded",
                code: BlockCode::QuotaExceeded,
            }
        );
    }

    #[tokio::test]
    async fn block_decision_is_written_to_decision_audit_sink() {
        let tenant_id = uuid::Uuid::new_v4();
        let project_id = uuid::Uuid::new_v4();
        let store: Arc<dyn PolicyStore> = Arc::new(QueryQuotaPolicyStore {
            max_bytes: 1,
            grace_pct: 101,
            hard_block_pct: 103,
        });
        let usage: Arc<dyn UsageReader> = Arc::new(EmptyUsageReader);
        let threshold_sink: Arc<dyn ThresholdSink> = Arc::new(NoopThresholdSink);
        let audit_sink = Arc::new(CapturingDecisionAuditSink::default());
        let enforcer = DefaultPolicyEnforcer::new(store, usage, threshold_sink)
            .with_decision_audit_sink(audit_sink.clone());

        let decision = enforcer
            .check_query(QueryCtx {
                tenant_id,
                project_id,
                signal: SignalKind::Traces,
                start_micros: Some(0),
                end_micros: Some(1),
                estimated_scanned_bytes: Some(2),
                now_micros: 42,
            })
            .await;

        assert_eq!(
            decision,
            Decision::Block {
                reason: "query_quota_exceeded",
                code: BlockCode::QuotaExceeded,
            }
        );
        let events = audit_sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, tenant_id);
        assert_eq!(events[0].project_id, project_id);
        assert_eq!(events[0].operation, Operation::Query);
        assert_eq!(events[0].decision, DecisionSummary::Block);
        assert_eq!(events[0].reason, "query_quota_exceeded");
        assert_eq!(events[0].block_code, Some(BlockCode::QuotaExceeded));
    }
}
