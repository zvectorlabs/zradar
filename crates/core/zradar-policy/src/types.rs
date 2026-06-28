use zradar_models::WorkspaceId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    Traces,
    Logs,
    Metrics,
    Rum,
    SessionReplay,
    ErrorTracking,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Ingest,
    Query,
    Store,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageBasis {
    CompressedBytes,
    ScannedBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PolicyLimit {
    Rate {
        records_per_sec: Option<u64>,
        bytes_per_sec: Option<u64>,
    },
    Quota {
        max_bytes: i64,
        period_start: i64,
        period_end: Option<i64>,
        basis: UsageBasis,
    },
    Size {
        max_bytes: i64,
        basis: UsageBasis,
    },
    Retention {
        max_days: u32,
    },
    Window {
        max_query_days: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicySource {
    Api,
    File,
    Env,
    Crd,
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PolicyId(pub i64);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    pub id: Option<PolicyId>,
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub operation: Operation,
    pub limit: PolicyLimit,
    pub grace_pct: u8,
    pub hard_block_pct: u8,
    pub effective_from: i64,
    pub effective_until: Option<i64>,
    pub source: PolicySource,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedPolicy {
    pub blocked: bool,
    pub rate: Option<PolicyLimit>,
    pub quotas: Vec<PolicyLimit>,
    pub size: Option<PolicyLimit>,
    pub retention: Option<PolicyLimit>,
    pub query_window: Option<PolicyLimit>,
    pub grace_pct: u8,
    pub hard_block_pct: u8,
}

impl Default for ResolvedPolicy {
    fn default() -> Self {
        Self {
            blocked: false,
            rate: None,
            quotas: Vec::new(),
            size: None,
            retention: None,
            query_window: None,
            grace_pct: 101,
            hard_block_pct: 103,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockCode {
    ProjectBlocked,
    RateLimitExceeded,
    QuotaExceeded,
    SizeExceeded,
    RetentionViolation,
    QueryWindowViolation,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Allow,
    AllowWithGrace {
        reason: &'static str,
    },
    Throttle {
        retry_after_ms: u32,
        reason: &'static str,
    },
    Block {
        reason: &'static str,
        code: BlockCode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionSummary {
    Allow,
    Grace,
    Throttle,
    Block,
}

impl From<&Decision> for DecisionSummary {
    fn from(value: &Decision) -> Self {
        match value {
            Decision::Allow => Self::Allow,
            Decision::AllowWithGrace { .. } => Self::Grace,
            Decision::Throttle { .. } => Self::Throttle,
            Decision::Block { .. } => Self::Block,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IngestCtx {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub records: u64,
    pub estimated_bytes: Option<u64>,
    pub now_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryCtx {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub start_micros: Option<i64>,
    pub end_micros: Option<i64>,
    pub estimated_scanned_bytes: Option<u64>,
    pub now_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RateSample {
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub sampled_at_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetentionUsageBucket {
    pub signal: SignalKind,
    pub retention_period_index: u32,
    pub compressed_bytes: i64,
    pub records: i64,
    pub file_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteSample {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub stream_name: Option<String>,
    pub compressed_bytes: i64,
    pub original_bytes: Option<i64>,
    pub records: i64,
    pub file_id: Option<i64>,
    pub decision: DecisionSummary,
    pub flushed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QuerySample {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub bytes_scanned: i64,
    pub rows_scanned: Option<i64>,
    pub query_time_ms: Option<i32>,
    pub decision: DecisionSummary,
    pub submitted_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdStatus {
    Ok,
    Warning,
    Critical,
    Grace,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QuotaStatus {
    pub signal: SignalKind,
    pub operation: Operation,
    pub limit_kind: String,
    pub limit_value: i64,
    pub observed_value: i64,
    pub pct_consumed: f64,
    pub status: ThresholdStatus,
    pub period_start: Option<i64>,
    pub period_end: Option<i64>,
    pub projected_exhaustion_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThresholdEvent {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub operation: Operation,
    pub limit_kind: String,
    pub threshold_pct: u16,
    pub observed_value: i64,
    pub limit_value: i64,
    pub period_start: Option<i64>,
    pub emitted_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecisionAuditEvent {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub operation: Operation,
    pub decision: DecisionSummary,
    pub reason: String,
    pub observed_value: Option<i64>,
    pub limit_value: Option<i64>,
    pub block_code: Option<BlockCode>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageDailyRecord {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub operation: Operation,
    pub day: String,
    pub used_bytes: i64,
    pub records: i64,
    pub query_count: i64,
    pub file_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IngestRateRecord {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub window_start_micros: i64,
    pub window_end_micros: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryUsageRecord {
    pub workspace_id: WorkspaceId,
    pub signal: SignalKind,
    pub bytes_scanned: i64,
    pub rows_scanned: i64,
    pub query_count: i64,
    pub avg_query_time_ms: Option<f64>,
    pub window_start_micros: i64,
    pub window_end_micros: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_policy_defaults_to_no_limits() {
        let policy = ResolvedPolicy::default();
        assert!(!policy.blocked);
        assert!(policy.rate.is_none());
        assert!(policy.quotas.is_empty());
        assert_eq!(policy.grace_pct, 101);
        assert_eq!(policy.hard_block_pct, 103);
    }

    #[test]
    fn decision_summary_maps_all_variants() {
        assert_eq!(
            DecisionSummary::from(&Decision::Allow),
            DecisionSummary::Allow
        );
        assert_eq!(
            DecisionSummary::from(&Decision::AllowWithGrace { reason: "quota" }),
            DecisionSummary::Grace
        );
        assert_eq!(
            DecisionSummary::from(&Decision::Throttle {
                retry_after_ms: 1000,
                reason: "rate"
            }),
            DecisionSummary::Throttle
        );
        assert_eq!(
            DecisionSummary::from(&Decision::Block {
                reason: "quota",
                code: BlockCode::QuotaExceeded
            }),
            DecisionSummary::Block
        );
    }
}
