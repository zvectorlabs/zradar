use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::PolicyError;
use crate::traits::{UsageReader, UsageTracker};
use crate::types::{
    Operation, QuerySample, RateSample, RetentionUsageBucket, SignalKind, WriteSample,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UsageKey {
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
    operation: Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct StoredKey {
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
}

#[derive(Debug, Clone, Copy)]
struct UsagePoint {
    at_micros: i64,
    records: u64,
    bytes: u64,
}

#[derive(Debug, Default)]
struct UsageState {
    points: RwLock<VecDeque<UsagePoint>>,
}

#[derive(Debug, Default)]
pub struct InMemoryUsageTracker {
    events: DashMap<UsageKey, UsageState>,
    stored_compressed_bytes: DashMap<StoredKey, i64>,
}

impl InMemoryUsageTracker {
    pub fn new() -> Self {
        Self::default()
    }

    fn record_point(&self, key: UsageKey, point: UsagePoint) {
        let state = self.events.entry(key).or_default();
        state.points.write().push_back(point);
    }

    fn add_stored_compressed_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        compressed_bytes: i64,
    ) {
        self.stored_compressed_bytes
            .entry(StoredKey {
                tenant_id,
                project_id,
                signal,
            })
            .and_modify(|value| *value = value.saturating_add(compressed_bytes))
            .or_insert(compressed_bytes);
    }
}

#[async_trait]
impl UsageTracker for InMemoryUsageTracker {
    async fn record_write(&self, sample: WriteSample) {
        let records = u64::try_from(sample.records).unwrap_or(0);
        let bytes = u64::try_from(sample.compressed_bytes).unwrap_or(0);
        self.record_point(
            UsageKey {
                tenant_id: sample.tenant_id,
                project_id: sample.project_id,
                signal: sample.signal,
                operation: Operation::Ingest,
            },
            UsagePoint {
                at_micros: sample.flushed_at,
                records,
                bytes,
            },
        );
        self.add_stored_compressed_bytes(
            sample.tenant_id,
            sample.project_id,
            sample.signal,
            sample.compressed_bytes,
        );
    }

    async fn record_query(&self, sample: QuerySample) {
        self.record_point(
            UsageKey {
                tenant_id: sample.tenant_id,
                project_id: sample.project_id,
                signal: sample.signal,
                operation: Operation::Query,
            },
            UsagePoint {
                at_micros: sample.submitted_at,
                records: 1,
                bytes: u64::try_from(sample.bytes_scanned).unwrap_or(0),
            },
        );
    }
}

#[async_trait]
impl UsageReader for InMemoryUsageTracker {
    async fn current_rate(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
    ) -> Result<RateSample, PolicyError> {
        let now = chrono::Utc::now().timestamp_micros();
        let cutoff = now.saturating_sub(1_000_000);
        let mut records_per_sec = 0_u64;
        let mut bytes_per_sec = 0_u64;

        for entry in self.events.iter() {
            if !usage_key_matches(*entry.key(), tenant_id, project_id, signal, operation) {
                continue;
            }

            for point in entry.value().points.read().iter() {
                if point.at_micros >= cutoff {
                    records_per_sec = records_per_sec.saturating_add(point.records);
                    bytes_per_sec = bytes_per_sec.saturating_add(point.bytes);
                }
            }
        }

        Ok(RateSample {
            records_per_sec,
            bytes_per_sec,
            sampled_at_micros: now,
        })
    }

    async fn period_used_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
        period_start: i64,
        period_end: Option<i64>,
    ) -> Result<i64, PolicyError> {
        let mut used = 0_i64;

        for entry in self.events.iter() {
            if !usage_key_matches(*entry.key(), tenant_id, project_id, signal, operation) {
                continue;
            }

            for point in entry.value().points.read().iter() {
                if point.at_micros >= period_start
                    && period_end.is_none_or(|period_end| point.at_micros < period_end)
                {
                    used = used.saturating_add(i64::try_from(point.bytes).unwrap_or(i64::MAX));
                }
            }
        }

        Ok(used)
    }

    async fn stored_compressed_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
    ) -> Result<i64, PolicyError> {
        let mut stored = 0_i64;

        for entry in self.stored_compressed_bytes.iter() {
            let key = *entry.key();
            if key.tenant_id == tenant_id
                && key.project_id == project_id
                && (signal == SignalKind::All || key.signal == signal)
            {
                stored = stored.saturating_add(*entry.value());
            }
        }

        Ok(stored)
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

pub struct FanoutUsageTracker {
    targets: Vec<Arc<dyn UsageTracker>>,
}

impl FanoutUsageTracker {
    pub fn new(targets: Vec<Arc<dyn UsageTracker>>) -> Self {
        Self { targets }
    }
}

#[async_trait]
impl UsageTracker for FanoutUsageTracker {
    async fn record_write(&self, sample: WriteSample) {
        for target in &self.targets {
            target.record_write(sample.clone()).await;
        }
    }

    async fn record_query(&self, sample: QuerySample) {
        for target in &self.targets {
            target.record_query(sample.clone()).await;
        }
    }
}

fn usage_key_matches(
    key: UsageKey,
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
    operation: Operation,
) -> bool {
    key.tenant_id == tenant_id
        && key.project_id == project_id
        && (signal == SignalKind::All || key.signal == signal)
        && (operation == Operation::All || key.operation == operation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DecisionSummary;

    #[tokio::test]
    async fn records_current_ingest_rate_and_period_usage() {
        let tracker = InMemoryUsageTracker::new();
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let now = chrono::Utc::now().timestamp_micros();

        tracker
            .record_write(WriteSample {
                tenant_id,
                project_id,
                signal: SignalKind::Traces,
                stream_name: None,
                compressed_bytes: 128,
                original_bytes: Some(256),
                records: 3,
                file_id: Some(1),
                decision: DecisionSummary::Allow,
                flushed_at: now,
            })
            .await;

        let rate = tracker
            .current_rate(tenant_id, project_id, SignalKind::Traces, Operation::Ingest)
            .await
            .unwrap();
        assert_eq!(rate.records_per_sec, 3);
        assert_eq!(rate.bytes_per_sec, 128);

        let used = tracker
            .period_used_bytes(
                tenant_id,
                project_id,
                SignalKind::Traces,
                Operation::Ingest,
                now - 1,
                None,
            )
            .await
            .unwrap();
        assert_eq!(used, 128);
    }

    #[tokio::test]
    async fn aggregates_all_signals_for_period_usage_and_stored_bytes() {
        let tracker = InMemoryUsageTracker::new();
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let now = chrono::Utc::now().timestamp_micros();

        for (signal, compressed_bytes) in [(SignalKind::Traces, 100), (SignalKind::Logs, 50)] {
            tracker
                .record_write(WriteSample {
                    tenant_id,
                    project_id,
                    signal,
                    stream_name: None,
                    compressed_bytes,
                    original_bytes: None,
                    records: 1,
                    file_id: None,
                    decision: DecisionSummary::Allow,
                    flushed_at: now,
                })
                .await;
        }

        let used = tracker
            .period_used_bytes(
                tenant_id,
                project_id,
                SignalKind::All,
                Operation::Ingest,
                now - 1,
                None,
            )
            .await
            .unwrap();
        assert_eq!(used, 150);

        let stored = tracker
            .stored_compressed_bytes(tenant_id, project_id, SignalKind::All)
            .await
            .unwrap();
        assert_eq!(stored, 150);
    }
}
