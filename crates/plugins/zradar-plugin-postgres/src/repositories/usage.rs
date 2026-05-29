use crate::client::PostgresClient;
use async_trait::async_trait;
use chrono::{Datelike, TimeZone};
use sqlx::{Postgres, QueryBuilder, Row};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, interval};
use tracing::{debug, warn};
use uuid::Uuid;
use zradar_policy::{
    BlockCode, DecisionAuditEvent, DecisionSummary, IngestRateRecord, Operation, PolicyError,
    QuerySample, QueryUsageRecord, RateSample, RetentionUsageBucket, SignalKind, ThresholdEvent,
    ThresholdSink, UsageAnalyticsReader, UsageDailyRecord, UsageReader, UsageTracker, WriteSample,
};

const DEFAULT_USAGE_CHANNEL_CAPACITY: usize = 16_384;
const DEFAULT_USAGE_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_USAGE_BATCH_SIZE: usize = 512;
const DEFAULT_USAGE_RETRY_BUFFER_MAX_SAMPLES: usize = 16_384;
const DEFAULT_USAGE_MAX_FLUSH_RETRIES: usize = 3;

#[derive(Debug, Default)]
pub struct UsageTrackerMetrics {
    write_samples_dropped_total: AtomicU64,
    query_samples_dropped_total: AtomicU64,
    flush_failures_total: AtomicU64,
    flush_batches_total: AtomicU64,
    flush_samples_total: AtomicU64,
    buffered_samples: AtomicU64,
    last_flush_duration_ms: AtomicU64,
}

impl UsageTrackerMetrics {
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);
        write_counter(
            &mut out,
            "zradar_usage_tracker_write_samples_dropped_total",
            self.write_samples_dropped_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_usage_tracker_query_samples_dropped_total",
            self.query_samples_dropped_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_usage_tracker_flush_failures_total",
            self.flush_failures_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_usage_tracker_flush_batches_total",
            self.flush_batches_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_usage_tracker_flush_samples_total",
            self.flush_samples_total.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_usage_tracker_buffered_samples",
            self.buffered_samples.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_usage_tracker_last_flush_duration_ms",
            self.last_flush_duration_ms.load(Ordering::Relaxed),
        );
        out
    }
}

fn write_counter(out: &mut String, name: &str, value: u64) {
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn write_gauge(out: &mut String, name: &str, value: u64) {
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

pub struct PostgresUsageTracker {
    sender: mpsc::Sender<UsageEvent>,
    metrics: Arc<UsageTrackerMetrics>,
}

impl PostgresUsageTracker {
    pub fn spawn(client: Arc<PostgresClient>) -> Self {
        Self::spawn_with_metrics(client, Arc::new(UsageTrackerMetrics::default()))
    }

    pub fn spawn_with_metrics(
        client: Arc<PostgresClient>,
        metrics: Arc<UsageTrackerMetrics>,
    ) -> Self {
        Self::spawn_with_options(
            client,
            metrics,
            DEFAULT_USAGE_CHANNEL_CAPACITY,
            DEFAULT_USAGE_FLUSH_INTERVAL_MS,
            DEFAULT_USAGE_BATCH_SIZE,
            DEFAULT_USAGE_RETRY_BUFFER_MAX_SAMPLES,
        )
    }

    pub fn spawn_with_options(
        client: Arc<PostgresClient>,
        metrics: Arc<UsageTrackerMetrics>,
        channel_capacity: usize,
        flush_interval_ms: u64,
        batch_size: usize,
        retry_buffer_max_samples: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let worker = PostgresUsageFlushWorker {
            client,
            metrics: metrics.clone(),
            receiver,
            flush_interval_ms,
            batch_size,
            retry_buffer_max_samples,
        };
        tokio::spawn(worker.run());
        Self { sender, metrics }
    }

    pub fn metrics(&self) -> Arc<UsageTrackerMetrics> {
        self.metrics.clone()
    }
}

#[async_trait]
impl UsageTracker for PostgresUsageTracker {
    async fn record_write(&self, sample: WriteSample) {
        if let Err(e) = self.sender.try_send(UsageEvent::Write(sample)) {
            self.metrics
                .write_samples_dropped_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(error = %e, "usage write sample dropped");
        }
    }

    async fn record_query(&self, sample: QuerySample) {
        if let Err(e) = self.sender.try_send(UsageEvent::Query(sample)) {
            self.metrics
                .query_samples_dropped_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(error = %e, "usage query sample dropped");
        }
    }
}

enum UsageEvent {
    Write(WriteSample),
    Query(QuerySample),
}

struct PostgresUsageFlushWorker {
    client: Arc<PostgresClient>,
    metrics: Arc<UsageTrackerMetrics>,
    receiver: mpsc::Receiver<UsageEvent>,
    flush_interval_ms: u64,
    batch_size: usize,
    retry_buffer_max_samples: usize,
}

impl PostgresUsageFlushWorker {
    async fn run(mut self) {
        let mut tick = interval(Duration::from_millis(self.flush_interval_ms));
        let mut writes = Vec::with_capacity(self.batch_size);
        let mut queries = Vec::with_capacity(self.batch_size);
        let mut write_flush_failures = 0_usize;
        let mut query_flush_failures = 0_usize;

        loop {
            tokio::select! {
                event = self.receiver.recv() => {
                    match event {
                        Some(UsageEvent::Write(sample)) => writes.push(sample),
                        Some(UsageEvent::Query(sample)) => queries.push(sample),
                        None => {
                            self.flush(
                                &mut writes,
                                &mut queries,
                                &mut write_flush_failures,
                                &mut query_flush_failures,
                            )
                            .await;
                            return;
                        }
                    }
                    self.record_buffered_samples(writes.len(), queries.len());
                    self.enforce_retry_buffer_limit(&mut writes, &mut queries);

                    if writes.len() + queries.len() >= self.batch_size {
                        self.flush(
                            &mut writes,
                            &mut queries,
                            &mut write_flush_failures,
                            &mut query_flush_failures,
                        )
                        .await;
                    }
                }
                _ = tick.tick() => {
                    self.flush(
                        &mut writes,
                        &mut queries,
                        &mut write_flush_failures,
                        &mut query_flush_failures,
                    )
                    .await;
                }
            }
        }
    }

    async fn flush(
        &self,
        writes: &mut Vec<WriteSample>,
        queries: &mut Vec<QuerySample>,
        write_flush_failures: &mut usize,
        query_flush_failures: &mut usize,
    ) {
        if writes.is_empty() && queries.is_empty() {
            return;
        }

        if !writes.is_empty() {
            let count = writes.len();
            let started_at = Instant::now();
            if let Err(e) = insert_write_samples(self.client.as_ref(), writes.as_slice()).await {
                self.metrics
                    .flush_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                *write_flush_failures = write_flush_failures.saturating_add(1);
                warn!(
                    error = %e,
                    count,
                    attempt = *write_flush_failures,
                    max_attempts = DEFAULT_USAGE_MAX_FLUSH_RETRIES,
                    "failed to flush usage write samples"
                );
                if *write_flush_failures >= DEFAULT_USAGE_MAX_FLUSH_RETRIES {
                    self.metrics
                        .write_samples_dropped_total
                        .fetch_add(count as u64, Ordering::Relaxed);
                    warn!(
                        count,
                        max_attempts = DEFAULT_USAGE_MAX_FLUSH_RETRIES,
                        "dropping usage write samples after repeated flush failures"
                    );
                    writes.clear();
                    *write_flush_failures = 0;
                }
            } else {
                self.metrics
                    .flush_batches_total
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .flush_samples_total
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.metrics.last_flush_duration_ms.store(
                    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                debug!(count, "flushed usage write samples");
                writes.clear();
                *write_flush_failures = 0;
            }
        }

        if !queries.is_empty() {
            let count = queries.len();
            let started_at = Instant::now();
            if let Err(e) = insert_query_samples(self.client.as_ref(), queries.as_slice()).await {
                self.metrics
                    .flush_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                *query_flush_failures = query_flush_failures.saturating_add(1);
                warn!(
                    error = %e,
                    count,
                    attempt = *query_flush_failures,
                    max_attempts = DEFAULT_USAGE_MAX_FLUSH_RETRIES,
                    "failed to flush usage query samples"
                );
                if *query_flush_failures >= DEFAULT_USAGE_MAX_FLUSH_RETRIES {
                    self.metrics
                        .query_samples_dropped_total
                        .fetch_add(count as u64, Ordering::Relaxed);
                    warn!(
                        count,
                        max_attempts = DEFAULT_USAGE_MAX_FLUSH_RETRIES,
                        "dropping usage query samples after repeated flush failures"
                    );
                    queries.clear();
                    *query_flush_failures = 0;
                }
            } else {
                self.metrics
                    .flush_batches_total
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .flush_samples_total
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.metrics.last_flush_duration_ms.store(
                    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                debug!(count, "flushed usage query samples");
                queries.clear();
                *query_flush_failures = 0;
            }
        }

        self.record_buffered_samples(writes.len(), queries.len());
        self.enforce_retry_buffer_limit(writes, queries);
    }

    fn record_buffered_samples(&self, write_count: usize, query_count: usize) {
        self.metrics.buffered_samples.store(
            u64::try_from(write_count.saturating_add(query_count)).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn enforce_retry_buffer_limit(
        &self,
        writes: &mut Vec<WriteSample>,
        queries: &mut Vec<QuerySample>,
    ) {
        let total_len = writes.len().saturating_add(queries.len());
        if total_len > self.retry_buffer_max_samples {
            let mut remaining = total_len - self.retry_buffer_max_samples;
            let mut write_len = writes.len();
            let mut query_len = queries.len();
            let mut writes_to_drop = 0_usize;
            let mut queries_to_drop = 0_usize;

            while remaining > 0 {
                if write_len >= query_len && write_len > 0 {
                    writes_to_drop += 1;
                    write_len -= 1;
                } else if query_len > 0 {
                    queries_to_drop += 1;
                    query_len -= 1;
                } else {
                    break;
                }
                remaining -= 1;
            }

            if writes_to_drop > 0 {
                writes.drain(0..writes_to_drop);
                self.metrics.write_samples_dropped_total.fetch_add(
                    u64::try_from(writes_to_drop).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            if queries_to_drop > 0 {
                queries.drain(0..queries_to_drop);
                self.metrics.query_samples_dropped_total.fetch_add(
                    u64::try_from(queries_to_drop).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
        }
        self.record_buffered_samples(writes.len(), queries.len());
    }
}

pub struct PostgresUsageReader {
    client: Arc<PostgresClient>,
}

pub struct PostgresThresholdSink {
    client: Arc<PostgresClient>,
}

pub struct PostgresDecisionAuditSink {
    client: Arc<PostgresClient>,
}

impl PostgresThresholdSink {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

impl PostgresDecisionAuditSink {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ThresholdSink for PostgresThresholdSink {
    async fn emit(&self, event: ThresholdEvent) -> Result<(), PolicyError> {
        let period_start = threshold_dedupe_period_start(&event);
        sqlx::query(
            r#"
            INSERT INTO threshold_dedupe (
                tenant_id,
                project_id,
                signal_kind,
                operation,
                limit_kind,
                threshold_pct,
                period_start,
                emitted_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (
                tenant_id,
                project_id,
                signal_kind,
                operation,
                limit_kind,
                threshold_pct,
                period_start
            ) DO NOTHING
            "#,
        )
        .bind(event.tenant_id)
        .bind(event.project_id)
        .bind(signal_kind(event.signal))
        .bind(operation_kind(event.operation))
        .bind(event.limit_kind)
        .bind(i16::try_from(event.threshold_pct).unwrap_or(i16::MAX))
        .bind(period_start)
        .bind(event.emitted_at)
        .execute(self.client.pool())
        .await
        .map_err(|e| PolicyError::ThresholdSinkFailed(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl zradar_policy::DecisionAuditSink for PostgresDecisionAuditSink {
    async fn record(&self, event: DecisionAuditEvent) -> Result<(), PolicyError> {
        sqlx::query(
            r#"
            INSERT INTO policy_decisions_audit (
                tenant_id,
                project_id,
                signal_kind,
                operation,
                decision,
                reason,
                observed_value,
                limit_value,
                block_code,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(event.tenant_id)
        .bind(event.project_id)
        .bind(signal_kind(event.signal))
        .bind(operation_kind(event.operation))
        .bind(decision_summary(event.decision))
        .bind(event.reason)
        .bind(event.observed_value)
        .bind(event.limit_value)
        .bind(event.block_code.map(block_code_kind))
        .bind(event.created_at)
        .execute(self.client.pool())
        .await
        .map_err(|e| PolicyError::DecisionAuditSinkFailed(e.to_string()))?;

        Ok(())
    }
}

impl PostgresUsageReader {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl UsageReader for PostgresUsageReader {
    async fn current_rate(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
    ) -> Result<RateSample, PolicyError> {
        let now = chrono::Utc::now().timestamp_micros();
        let cutoff = now.saturating_sub(1_000_000);

        let (records_per_sec, bytes_per_sec) = match operation {
            Operation::Ingest => {
                let row = if signal == SignalKind::All {
                    sqlx::query(
                        r#"
                        SELECT
                            COALESCE(SUM(records), 0)::bigint AS records_per_sec,
                            COALESCE(SUM(compressed_bytes), 0)::bigint AS bytes_per_sec
                        FROM ingestion_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND flushed_at >= $3
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(cutoff)
                    .fetch_one(self.client.pool())
                    .await
                } else {
                    sqlx::query(
                        r#"
                        SELECT
                            COALESCE(SUM(records), 0)::bigint AS records_per_sec,
                            COALESCE(SUM(compressed_bytes), 0)::bigint AS bytes_per_sec
                        FROM ingestion_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND signal_kind = $3
                          AND flushed_at >= $4
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(signal_kind(signal))
                    .bind(cutoff)
                    .fetch_one(self.client.pool())
                    .await
                }
                .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

                (
                    row.get::<i64, _>("records_per_sec"),
                    row.get::<i64, _>("bytes_per_sec"),
                )
            }
            Operation::Query => {
                let row = if signal == SignalKind::All {
                    sqlx::query(
                        r#"
                        SELECT
                            COUNT(*)::bigint AS records_per_sec,
                            COALESCE(SUM(bytes_scanned), 0)::bigint AS bytes_per_sec
                        FROM query_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND submitted_at >= $3
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(cutoff)
                    .fetch_one(self.client.pool())
                    .await
                } else {
                    sqlx::query(
                        r#"
                        SELECT
                            COUNT(*)::bigint AS records_per_sec,
                            COALESCE(SUM(bytes_scanned), 0)::bigint AS bytes_per_sec
                        FROM query_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND signal_kind = $3
                          AND submitted_at >= $4
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(signal_kind(signal))
                    .bind(cutoff)
                    .fetch_one(self.client.pool())
                    .await
                }
                .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

                (
                    row.get::<i64, _>("records_per_sec"),
                    row.get::<i64, _>("bytes_per_sec"),
                )
            }
            Operation::Store | Operation::All => (0, 0),
        };

        Ok(RateSample {
            records_per_sec: u64::try_from(records_per_sec).unwrap_or(0),
            bytes_per_sec: u64::try_from(bytes_per_sec).unwrap_or(0),
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
        match operation {
            Operation::Ingest => {
                let row = if signal == SignalKind::All {
                    sqlx::query(
                        r#"
                        SELECT COALESCE(SUM(compressed_bytes), 0)::bigint AS used_bytes
                        FROM ingestion_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND flushed_at >= $3
                          AND ($4::bigint IS NULL OR flushed_at < $4)
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(period_start)
                    .bind(period_end)
                    .fetch_one(self.client.pool())
                    .await
                } else {
                    sqlx::query(
                        r#"
                        SELECT COALESCE(SUM(compressed_bytes), 0)::bigint AS used_bytes
                        FROM ingestion_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND signal_kind = $3
                          AND flushed_at >= $4
                          AND ($5::bigint IS NULL OR flushed_at < $5)
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(signal_kind(signal))
                    .bind(period_start)
                    .bind(period_end)
                    .fetch_one(self.client.pool())
                    .await
                }
                .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

                Ok(row.get::<i64, _>("used_bytes"))
            }
            Operation::Query => {
                let row = if signal == SignalKind::All {
                    sqlx::query(
                        r#"
                        SELECT COALESCE(SUM(bytes_scanned), 0)::bigint AS used_bytes
                        FROM query_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND submitted_at >= $3
                          AND ($4::bigint IS NULL OR submitted_at < $4)
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(period_start)
                    .bind(period_end)
                    .fetch_one(self.client.pool())
                    .await
                } else {
                    sqlx::query(
                        r#"
                        SELECT COALESCE(SUM(bytes_scanned), 0)::bigint AS used_bytes
                        FROM query_events
                        WHERE tenant_id = $1
                          AND project_id = $2
                          AND signal_kind = $3
                          AND submitted_at >= $4
                          AND ($5::bigint IS NULL OR submitted_at < $5)
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(project_id)
                    .bind(signal_kind(signal))
                    .bind(period_start)
                    .bind(period_end)
                    .fetch_one(self.client.pool())
                    .await
                }
                .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

                Ok(row.get::<i64, _>("used_bytes"))
            }
            Operation::Store | Operation::All => Ok(0),
        }
    }

    async fn stored_compressed_bytes(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
    ) -> Result<i64, PolicyError> {
        let row = if signal == SignalKind::All {
            sqlx::query(
                r#"
                SELECT COALESCE(SUM(compressed_size), 0)::bigint AS stored_bytes
                FROM file_list
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND deleted = false
                "#,
            )
            .bind(tenant_id)
            .bind(project_id)
            .fetch_one(self.client.pool())
            .await
        } else {
            sqlx::query(
                r#"
                SELECT COALESCE(SUM(compressed_size), 0)::bigint AS stored_bytes
                FROM file_list
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND signal_type = $3
                  AND deleted = false
                "#,
            )
            .bind(tenant_id)
            .bind(project_id)
            .bind(signal_kind(signal))
            .fetch_one(self.client.pool())
            .await
        }
        .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

        Ok(row.get::<i64, _>("stored_bytes"))
    }

    async fn retention_buckets(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
    ) -> Result<Vec<RetentionUsageBucket>, PolicyError> {
        let now = chrono::Utc::now().timestamp_micros();
        let bucket_micros = 30_i64 * 86_400 * 1_000_000;

        let rows = if signal == SignalKind::All {
            sqlx::query(
                r#"
                SELECT
                    signal_type,
                    GREATEST((($3::bigint - max_ts) / $4::bigint), 0)::bigint AS bucket_index,
                    COALESCE(SUM(compressed_size), 0)::bigint AS compressed_bytes,
                    COALESCE(SUM(records), 0)::bigint AS records,
                    COUNT(*)::bigint AS file_count
                FROM file_list
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND deleted = false
                GROUP BY signal_type, bucket_index
                ORDER BY signal_type, bucket_index
                "#,
            )
            .bind(tenant_id)
            .bind(project_id)
            .bind(now)
            .bind(bucket_micros)
            .fetch_all(self.client.pool())
            .await
        } else {
            sqlx::query(
                r#"
                SELECT
                    signal_type,
                    GREATEST((($4::bigint - max_ts) / $5::bigint), 0)::bigint AS bucket_index,
                    COALESCE(SUM(compressed_size), 0)::bigint AS compressed_bytes,
                    COALESCE(SUM(records), 0)::bigint AS records,
                    COUNT(*)::bigint AS file_count
                FROM file_list
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND signal_type = $3
                  AND deleted = false
                GROUP BY signal_type, bucket_index
                ORDER BY signal_type, bucket_index
                "#,
            )
            .bind(tenant_id)
            .bind(project_id)
            .bind(signal_kind(signal))
            .bind(now)
            .bind(bucket_micros)
            .fetch_all(self.client.pool())
            .await
        }
        .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let signal_type = row.get::<String, _>("signal_type");
                Ok(RetentionUsageBucket {
                    signal: parse_signal_kind(&signal_type)?,
                    retention_period_index: u32::try_from(row.get::<i64, _>("bucket_index"))
                        .unwrap_or(0),
                    compressed_bytes: row.get::<i64, _>("compressed_bytes"),
                    records: row.get::<i64, _>("records"),
                    file_count: row.get::<i64, _>("file_count"),
                })
            })
            .collect()
    }
}

#[async_trait]
impl UsageAnalyticsReader for PostgresUsageReader {
    async fn usage_daily(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: Option<SignalKind>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> Result<Vec<UsageDailyRecord>, PolicyError> {
        let signal_filter = signal
            .filter(|signal| *signal != SignalKind::All)
            .map(signal_kind);
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                project_id,
                signal_kind,
                'ingest' AS operation,
                day::text AS day,
                compressed_bytes AS used_bytes,
                records,
                0::bigint AS query_count,
                file_count
            FROM ingestion_daily
            WHERE tenant_id = $1
              AND project_id = $2
              AND ($3::text IS NULL OR signal_kind = $3)
              AND ($4::bigint IS NULL OR day >= (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
              AND ($5::bigint IS NULL OR day <= (to_timestamp($5::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
            UNION ALL
            SELECT
                tenant_id,
                project_id,
                signal_kind,
                'query' AS operation,
                day::text AS day,
                bytes_scanned AS used_bytes,
                rows_scanned AS records,
                query_count,
                0::bigint AS file_count
            FROM query_usage_daily
            WHERE tenant_id = $1
              AND project_id = $2
              AND ($3::text IS NULL OR signal_kind = $3)
              AND ($4::bigint IS NULL OR day >= (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
              AND ($5::bigint IS NULL OR day <= (to_timestamp($5::double precision / 1000000.0) AT TIME ZONE 'UTC')::date)
            ORDER BY day DESC, signal_kind, operation
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(signal_filter)
        .bind(start_micros)
        .bind(end_micros)
        .fetch_all(self.client.pool())
        .await
        .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let signal_kind = row.get::<String, _>("signal_kind");
                let operation = row.get::<String, _>("operation");
                Ok(UsageDailyRecord {
                    tenant_id: row.get("tenant_id"),
                    project_id: row.get("project_id"),
                    signal: parse_signal_kind(&signal_kind)?,
                    operation: parse_operation_kind(&operation)?,
                    day: row.get("day"),
                    used_bytes: row.get("used_bytes"),
                    records: row.get("records"),
                    query_count: row.get("query_count"),
                    file_count: row.get("file_count"),
                })
            })
            .collect()
    }

    async fn ingest_rate(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: Option<SignalKind>,
        window_start_micros: i64,
        window_end_micros: i64,
    ) -> Result<Vec<IngestRateRecord>, PolicyError> {
        let signal_filter = signal
            .filter(|signal| *signal != SignalKind::All)
            .map(signal_kind);
        let window_seconds =
            ((window_end_micros.saturating_sub(window_start_micros)) / 1_000_000).max(1);
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                project_id,
                signal_kind,
                COALESCE(SUM(records), 0)::bigint AS records,
                COALESCE(SUM(compressed_bytes), 0)::bigint AS compressed_bytes
            FROM ingestion_events
            WHERE tenant_id = $1
              AND project_id = $2
              AND ($3::text IS NULL OR signal_kind = $3)
              AND flushed_at >= $4
              AND flushed_at < $5
            GROUP BY tenant_id, project_id, signal_kind
            ORDER BY signal_kind
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(signal_filter)
        .bind(window_start_micros)
        .bind(window_end_micros)
        .fetch_all(self.client.pool())
        .await
        .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let signal_kind = row.get::<String, _>("signal_kind");
                let records = row.get::<i64, _>("records");
                let compressed_bytes = row.get::<i64, _>("compressed_bytes");
                Ok(IngestRateRecord {
                    tenant_id: row.get("tenant_id"),
                    project_id: row.get("project_id"),
                    signal: parse_signal_kind(&signal_kind)?,
                    records_per_sec: u64::try_from(records / window_seconds).unwrap_or(0),
                    bytes_per_sec: u64::try_from(compressed_bytes / window_seconds).unwrap_or(0),
                    window_start_micros,
                    window_end_micros,
                })
            })
            .collect()
    }

    async fn query_usage(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: Option<SignalKind>,
        window_start_micros: i64,
        window_end_micros: i64,
    ) -> Result<Vec<QueryUsageRecord>, PolicyError> {
        let signal_filter = signal
            .filter(|signal| *signal != SignalKind::All)
            .map(signal_kind);
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                project_id,
                signal_kind,
                COALESCE(SUM(bytes_scanned), 0)::bigint AS bytes_scanned,
                COALESCE(SUM(rows_scanned), 0)::bigint AS rows_scanned,
                COUNT(*)::bigint AS query_count,
                AVG(query_time_ms)::double precision AS avg_query_time_ms
            FROM query_events
            WHERE tenant_id = $1
              AND project_id = $2
              AND ($3::text IS NULL OR signal_kind = $3)
              AND submitted_at >= $4
              AND submitted_at < $5
            GROUP BY tenant_id, project_id, signal_kind
            ORDER BY signal_kind
            "#,
        )
        .bind(tenant_id)
        .bind(project_id)
        .bind(signal_filter)
        .bind(window_start_micros)
        .bind(window_end_micros)
        .fetch_all(self.client.pool())
        .await
        .map_err(|e| PolicyError::UsageUnavailable(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let signal_kind = row.get::<String, _>("signal_kind");
                Ok(QueryUsageRecord {
                    tenant_id: row.get("tenant_id"),
                    project_id: row.get("project_id"),
                    signal: parse_signal_kind(&signal_kind)?,
                    bytes_scanned: row.get("bytes_scanned"),
                    rows_scanned: row.get("rows_scanned"),
                    query_count: row.get("query_count"),
                    avg_query_time_ms: row.get("avg_query_time_ms"),
                    window_start_micros,
                    window_end_micros,
                })
            })
            .collect()
    }
}

fn signal_kind(signal: SignalKind) -> &'static str {
    match signal {
        SignalKind::Traces => "traces",
        SignalKind::Logs => "logs",
        SignalKind::Metrics => "metrics",
        SignalKind::Rum => "rum",
        SignalKind::SessionReplay => "session_replay",
        SignalKind::ErrorTracking => "error_tracking",
        SignalKind::All => "all",
    }
}

fn decision_summary(decision: DecisionSummary) -> &'static str {
    match decision {
        DecisionSummary::Allow => "allow",
        DecisionSummary::Grace => "grace",
        DecisionSummary::Throttle => "throttle",
        DecisionSummary::Block => "block",
    }
}

fn block_code_kind(code: BlockCode) -> &'static str {
    match code {
        BlockCode::ProjectBlocked => "project_blocked",
        BlockCode::RateLimitExceeded => "rate_limit_exceeded",
        BlockCode::QuotaExceeded => "quota_exceeded",
        BlockCode::SizeExceeded => "size_exceeded",
        BlockCode::RetentionViolation => "retention_violation",
        BlockCode::QueryWindowViolation => "query_window_violation",
    }
}

async fn insert_write_samples(
    client: &PostgresClient,
    samples: &[WriteSample],
) -> Result<(), sqlx::Error> {
    if samples.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO ingestion_events (
            tenant_id,
            project_id,
            signal_kind,
            stream_name,
            compressed_bytes,
            original_bytes,
            records,
            file_id,
            decision,
            flushed_at
        )
        "#,
    );

    builder.push_values(samples, |mut b, sample| {
        b.push_bind(sample.tenant_id)
            .push_bind(sample.project_id)
            .push_bind(signal_kind(sample.signal))
            .push_bind(sample.stream_name.as_deref())
            .push_bind(sample.compressed_bytes)
            .push_bind(sample.original_bytes)
            .push_bind(sample.records)
            .push_bind(sample.file_id)
            .push_bind(decision_summary(sample.decision))
            .push_bind(sample.flushed_at);
    });

    let mut tx = client.pool().begin().await?;
    builder.build().execute(&mut *tx).await?;
    upsert_ingestion_daily(&mut tx, samples).await?;
    upsert_ingest_query_monthly_for_writes(&mut tx, samples).await?;
    tx.commit().await?;
    Ok(())
}

async fn insert_query_samples(
    client: &PostgresClient,
    samples: &[QuerySample],
) -> Result<(), sqlx::Error> {
    if samples.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO query_events (
            tenant_id,
            project_id,
            signal_kind,
            bytes_scanned,
            rows_scanned,
            query_time_ms,
            decision,
            submitted_at
        )
        "#,
    );

    builder.push_values(samples, |mut b, sample| {
        b.push_bind(sample.tenant_id)
            .push_bind(sample.project_id)
            .push_bind(signal_kind(sample.signal))
            .push_bind(sample.bytes_scanned)
            .push_bind(sample.rows_scanned)
            .push_bind(sample.query_time_ms)
            .push_bind(decision_summary(sample.decision))
            .push_bind(sample.submitted_at);
    });

    let mut tx = client.pool().begin().await?;
    builder.build().execute(&mut *tx).await?;
    upsert_query_usage_daily(&mut tx, samples).await?;
    upsert_ingest_query_monthly_for_queries(&mut tx, samples).await?;
    tx.commit().await?;
    Ok(())
}

struct IngestionDailyAggregate {
    tenant_id: Uuid,
    project_id: Uuid,
    signal_kind: &'static str,
    day_micros: i64,
    compressed_bytes: i64,
    original_bytes: i64,
    records: i64,
    file_count: i64,
    updated_at: i64,
}

struct QueryDailyAggregate {
    tenant_id: Uuid,
    project_id: Uuid,
    signal_kind: &'static str,
    day_micros: i64,
    bytes_scanned: i64,
    rows_scanned: i64,
    query_count: i64,
    updated_at: i64,
}

struct MonthlyUsageAggregate {
    tenant_id: Uuid,
    project_id: Uuid,
    signal_kind: &'static str,
    operation: &'static str,
    period_start_micros: i64,
    used_bytes: i64,
    updated_at: i64,
}

async fn upsert_ingestion_daily(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[WriteSample],
) -> Result<(), sqlx::Error> {
    let aggregates = aggregate_ingestion_daily(samples);
    if aggregates.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO ingestion_daily (
            tenant_id,
            project_id,
            signal_kind,
            day,
            compressed_bytes,
            original_bytes,
            records,
            file_count,
            updated_at
        )
        "#,
    );

    push_ingestion_daily_values(&mut builder, &aggregates);

    builder.push(
        r#"
        ON CONFLICT (tenant_id, project_id, signal_kind, day)
        DO UPDATE SET
            compressed_bytes = ingestion_daily.compressed_bytes + EXCLUDED.compressed_bytes,
            original_bytes = ingestion_daily.original_bytes + EXCLUDED.original_bytes,
            records = ingestion_daily.records + EXCLUDED.records,
            file_count = ingestion_daily.file_count + EXCLUDED.file_count,
            updated_at = GREATEST(ingestion_daily.updated_at, EXCLUDED.updated_at)
        "#,
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

fn aggregate_ingestion_daily(samples: &[WriteSample]) -> Vec<IngestionDailyAggregate> {
    let mut aggregates = HashMap::<(Uuid, Uuid, &'static str, i64), IngestionDailyAggregate>::new();

    for sample in samples {
        let signal_kind = signal_kind(sample.signal);
        let day_micros = day_start_micros(sample.flushed_at);
        let entry = aggregates
            .entry((sample.tenant_id, sample.project_id, signal_kind, day_micros))
            .or_insert(IngestionDailyAggregate {
                tenant_id: sample.tenant_id,
                project_id: sample.project_id,
                signal_kind,
                day_micros,
                compressed_bytes: 0,
                original_bytes: 0,
                records: 0,
                file_count: 0,
                updated_at: sample.flushed_at,
            });

        entry.compressed_bytes = entry
            .compressed_bytes
            .saturating_add(sample.compressed_bytes);
        entry.original_bytes = entry
            .original_bytes
            .saturating_add(sample.original_bytes.unwrap_or(0));
        entry.records = entry.records.saturating_add(sample.records);
        entry.file_count = entry.file_count.saturating_add(1);
        entry.updated_at = entry.updated_at.max(sample.flushed_at);
    }

    aggregates.into_values().collect()
}

fn push_ingestion_daily_values(
    builder: &mut QueryBuilder<Postgres>,
    aggregates: &[IngestionDailyAggregate],
) {
    builder.push(" VALUES ");

    for (idx, aggregate) in aggregates.iter().enumerate() {
        if idx > 0 {
            builder.push(", ");
        }
        builder.push("(");
        builder.push_bind(aggregate.tenant_id);
        builder.push(", ");
        builder.push_bind(aggregate.project_id);
        builder.push(", ");
        builder.push_bind(aggregate.signal_kind);
        builder.push(", (to_timestamp(");
        builder.push_bind(aggregate.day_micros);
        builder.push("::double precision / 1000000.0) AT TIME ZONE 'UTC')::date, ");
        builder.push_bind(aggregate.compressed_bytes);
        builder.push(", ");
        builder.push_bind(aggregate.original_bytes);
        builder.push(", ");
        builder.push_bind(aggregate.records);
        builder.push(", ");
        builder.push_bind(aggregate.file_count);
        builder.push(", ");
        builder.push_bind(aggregate.updated_at);
        builder.push(")");
    }
}

async fn upsert_query_usage_daily(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[QuerySample],
) -> Result<(), sqlx::Error> {
    let aggregates = aggregate_query_daily(samples);
    if aggregates.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO query_usage_daily (
            tenant_id,
            project_id,
            signal_kind,
            day,
            bytes_scanned,
            rows_scanned,
            query_count,
            updated_at
        )
        "#,
    );

    push_query_daily_values(&mut builder, &aggregates);

    builder.push(
        r#"
        ON CONFLICT (tenant_id, project_id, signal_kind, day)
        DO UPDATE SET
            bytes_scanned = query_usage_daily.bytes_scanned + EXCLUDED.bytes_scanned,
            rows_scanned = query_usage_daily.rows_scanned + EXCLUDED.rows_scanned,
            query_count = query_usage_daily.query_count + EXCLUDED.query_count,
            updated_at = GREATEST(query_usage_daily.updated_at, EXCLUDED.updated_at)
        "#,
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

fn aggregate_query_daily(samples: &[QuerySample]) -> Vec<QueryDailyAggregate> {
    let mut aggregates = HashMap::<(Uuid, Uuid, &'static str, i64), QueryDailyAggregate>::new();

    for sample in samples {
        let signal_kind = signal_kind(sample.signal);
        let day_micros = day_start_micros(sample.submitted_at);
        let entry = aggregates
            .entry((sample.tenant_id, sample.project_id, signal_kind, day_micros))
            .or_insert(QueryDailyAggregate {
                tenant_id: sample.tenant_id,
                project_id: sample.project_id,
                signal_kind,
                day_micros,
                bytes_scanned: 0,
                rows_scanned: 0,
                query_count: 0,
                updated_at: sample.submitted_at,
            });

        entry.bytes_scanned = entry.bytes_scanned.saturating_add(sample.bytes_scanned);
        entry.rows_scanned = entry
            .rows_scanned
            .saturating_add(sample.rows_scanned.unwrap_or(0));
        entry.query_count = entry.query_count.saturating_add(1);
        entry.updated_at = entry.updated_at.max(sample.submitted_at);
    }

    aggregates.into_values().collect()
}

fn push_query_daily_values(
    builder: &mut QueryBuilder<Postgres>,
    aggregates: &[QueryDailyAggregate],
) {
    builder.push(" VALUES ");

    for (idx, aggregate) in aggregates.iter().enumerate() {
        if idx > 0 {
            builder.push(", ");
        }
        builder.push("(");
        builder.push_bind(aggregate.tenant_id);
        builder.push(", ");
        builder.push_bind(aggregate.project_id);
        builder.push(", ");
        builder.push_bind(aggregate.signal_kind);
        builder.push(", (to_timestamp(");
        builder.push_bind(aggregate.day_micros);
        builder.push("::double precision / 1000000.0) AT TIME ZONE 'UTC')::date, ");
        builder.push_bind(aggregate.bytes_scanned);
        builder.push(", ");
        builder.push_bind(aggregate.rows_scanned);
        builder.push(", ");
        builder.push_bind(aggregate.query_count);
        builder.push(", ");
        builder.push_bind(aggregate.updated_at);
        builder.push(")");
    }
}

async fn upsert_ingest_query_monthly_for_writes(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[WriteSample],
) -> Result<(), sqlx::Error> {
    let aggregates = aggregate_monthly_usage_for_writes(samples);
    upsert_monthly_usage_aggregates(tx, aggregates).await
}

async fn upsert_ingest_query_monthly_for_queries(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[QuerySample],
) -> Result<(), sqlx::Error> {
    let aggregates = aggregate_monthly_usage_for_queries(samples);
    upsert_monthly_usage_aggregates(tx, aggregates).await
}

fn aggregate_monthly_usage_for_writes(samples: &[WriteSample]) -> Vec<MonthlyUsageAggregate> {
    let mut aggregates =
        HashMap::<(Uuid, Uuid, SignalKind, Operation, i64), MonthlyUsageAggregate>::new();

    for sample in samples {
        aggregate_monthly_usage(
            &mut aggregates,
            sample.tenant_id,
            sample.project_id,
            sample.signal,
            Operation::Ingest,
            sample.flushed_at,
            sample.compressed_bytes,
        );
    }

    aggregates.into_values().collect()
}

fn aggregate_monthly_usage_for_queries(samples: &[QuerySample]) -> Vec<MonthlyUsageAggregate> {
    let mut aggregates =
        HashMap::<(Uuid, Uuid, SignalKind, Operation, i64), MonthlyUsageAggregate>::new();

    for sample in samples {
        aggregate_monthly_usage(
            &mut aggregates,
            sample.tenant_id,
            sample.project_id,
            sample.signal,
            Operation::Query,
            sample.submitted_at,
            sample.bytes_scanned,
        );
    }

    aggregates.into_values().collect()
}

fn aggregate_monthly_usage(
    aggregates: &mut HashMap<(Uuid, Uuid, SignalKind, Operation, i64), MonthlyUsageAggregate>,
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
    operation: Operation,
    observed_at: i64,
    used_bytes: i64,
) {
    let period_start_micros = month_start_micros(observed_at);
    let entry = aggregates
        .entry((
            tenant_id,
            project_id,
            signal,
            operation,
            period_start_micros,
        ))
        .or_insert(MonthlyUsageAggregate {
            tenant_id,
            project_id,
            signal_kind: signal_kind(signal),
            operation: operation_kind(operation),
            period_start_micros,
            used_bytes: 0,
            updated_at: observed_at,
        });

    entry.used_bytes = entry.used_bytes.saturating_add(used_bytes);
    entry.updated_at = entry.updated_at.max(observed_at);
}

async fn upsert_monthly_usage_aggregates(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    aggregates: Vec<MonthlyUsageAggregate>,
) -> Result<(), sqlx::Error> {
    if aggregates.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"
        INSERT INTO ingest_query_monthly (
            tenant_id,
            project_id,
            signal_kind,
            operation,
            period_start,
            used_bytes,
            limit_bytes,
            last_breach_at,
            updated_at
        )
        "#,
    );

    push_monthly_usage_values(&mut builder, &aggregates);

    builder.push(
        r#"
        ON CONFLICT (tenant_id, project_id, signal_kind, operation, period_start)
        DO UPDATE SET
            used_bytes = ingest_query_monthly.used_bytes + EXCLUDED.used_bytes,
            updated_at = GREATEST(ingest_query_monthly.updated_at, EXCLUDED.updated_at)
        "#,
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

fn push_monthly_usage_values(
    builder: &mut QueryBuilder<Postgres>,
    aggregates: &[MonthlyUsageAggregate],
) {
    builder.push(" VALUES ");

    for (idx, aggregate) in aggregates.iter().enumerate() {
        if idx > 0 {
            builder.push(", ");
        }
        builder.push("(");
        builder.push_bind(aggregate.tenant_id);
        builder.push(", ");
        builder.push_bind(aggregate.project_id);
        builder.push(", ");
        builder.push_bind(aggregate.signal_kind);
        builder.push(", ");
        builder.push_bind(aggregate.operation);
        builder.push(", date_trunc('month', to_timestamp(");
        builder.push_bind(aggregate.period_start_micros);
        builder.push("::double precision / 1000000.0) AT TIME ZONE 'UTC')::date, ");
        builder.push_bind(aggregate.used_bytes);
        builder.push(", ");
        builder.push_bind(0_i64);
        builder.push(", ");
        builder.push_bind(None::<i64>);
        builder.push(", ");
        builder.push_bind(aggregate.updated_at);
        builder.push(")");
    }
}

fn operation_kind(operation: Operation) -> &'static str {
    match operation {
        Operation::Ingest => "ingest",
        Operation::Query => "query",
        Operation::Store => "store",
        Operation::All => "all",
    }
}

fn threshold_dedupe_period_start(event: &ThresholdEvent) -> i64 {
    const HOUR_MICROS: i64 = 3_600 * 1_000_000;
    event
        .period_start
        .unwrap_or_else(|| event.emitted_at - event.emitted_at.rem_euclid(HOUR_MICROS))
}

fn day_start_micros(timestamp_micros: i64) -> i64 {
    const DAY_MICROS: i64 = 86_400 * 1_000_000;
    timestamp_micros - timestamp_micros.rem_euclid(DAY_MICROS)
}

fn month_start_micros(timestamp_micros: i64) -> i64 {
    let Some(datetime) = chrono::DateTime::from_timestamp_micros(timestamp_micros) else {
        return timestamp_micros;
    };

    chrono::Utc
        .with_ymd_and_hms(datetime.year(), datetime.month(), 1, 0, 0, 0)
        .single()
        .map(|datetime| datetime.timestamp_micros())
        .unwrap_or(timestamp_micros)
}

fn parse_operation_kind(value: &str) -> Result<Operation, PolicyError> {
    match value {
        "ingest" => Ok(Operation::Ingest),
        "query" => Ok(Operation::Query),
        "store" => Ok(Operation::Store),
        "all" => Ok(Operation::All),
        _ => Err(PolicyError::UsageUnavailable(format!(
            "unknown operation: {value}"
        ))),
    }
}

fn parse_signal_kind(value: &str) -> Result<SignalKind, PolicyError> {
    match value {
        "traces" => Ok(SignalKind::Traces),
        "logs" => Ok(SignalKind::Logs),
        "metrics" => Ok(SignalKind::Metrics),
        "rum" => Ok(SignalKind::Rum),
        "session_replay" => Ok(SignalKind::SessionReplay),
        "error_tracking" => Ok(SignalKind::ErrorTracking),
        "all" => Ok(SignalKind::All),
        _ => Err(PolicyError::Invalid(format!("unknown signal_kind {value}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_sample(
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        compressed_bytes: i64,
        original_bytes: Option<i64>,
        records: i64,
        flushed_at: i64,
    ) -> WriteSample {
        WriteSample {
            tenant_id,
            project_id,
            signal,
            stream_name: None,
            compressed_bytes,
            original_bytes,
            records,
            file_id: None,
            decision: DecisionSummary::Allow,
            flushed_at,
        }
    }

    fn query_sample(
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        bytes_scanned: i64,
        rows_scanned: Option<i64>,
        submitted_at: i64,
    ) -> QuerySample {
        QuerySample {
            tenant_id,
            project_id,
            signal,
            bytes_scanned,
            rows_scanned,
            query_time_ms: None,
            decision: DecisionSummary::Allow,
            submitted_at,
        }
    }

    #[test]
    fn signal_kind_round_trips_supported_values() {
        assert_eq!(
            parse_signal_kind(signal_kind(SignalKind::Traces)).unwrap(),
            SignalKind::Traces
        );
        assert_eq!(
            parse_signal_kind(signal_kind(SignalKind::Logs)).unwrap(),
            SignalKind::Logs
        );
        assert_eq!(
            parse_signal_kind(signal_kind(SignalKind::Metrics)).unwrap(),
            SignalKind::Metrics
        );
    }

    #[test]
    fn usage_tracker_metrics_render_prometheus_names() {
        let metrics = UsageTrackerMetrics::default();
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("zradar_usage_tracker_write_samples_dropped_total 0"));
        assert!(rendered.contains("zradar_usage_tracker_query_samples_dropped_total 0"));
        assert!(rendered.contains("zradar_usage_tracker_flush_failures_total 0"));
        assert!(rendered.contains("zradar_usage_tracker_flush_batches_total 0"));
        assert!(rendered.contains("zradar_usage_tracker_flush_samples_total 0"));
        assert!(rendered.contains("zradar_usage_tracker_buffered_samples 0"));
    }

    #[test]
    fn threshold_dedupe_uses_period_or_hour_bucket() {
        let emitted_at = (5 * 3_600 * 1_000_000) + 123_456;
        let mut event = ThresholdEvent {
            tenant_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            signal: SignalKind::Traces,
            operation: Operation::Ingest,
            limit_kind: "rate".to_string(),
            threshold_pct: 70,
            observed_value: 70,
            limit_value: 100,
            period_start: None,
            emitted_at,
        };

        assert_eq!(threshold_dedupe_period_start(&event), 5 * 3_600 * 1_000_000);

        event.period_start = Some(42);
        assert_eq!(threshold_dedupe_period_start(&event), 42);
    }

    #[test]
    fn aggregate_ingestion_daily_groups_by_tenant_project_signal_day() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let day = 2 * 86_400 * 1_000_000;
        let aggregates = aggregate_ingestion_daily(&[
            write_sample(
                tenant_id,
                project_id,
                SignalKind::Traces,
                10,
                Some(20),
                3,
                day + 1,
            ),
            write_sample(
                tenant_id,
                project_id,
                SignalKind::Traces,
                15,
                None,
                4,
                day + 2,
            ),
            write_sample(
                tenant_id,
                project_id,
                SignalKind::Logs,
                7,
                Some(9),
                1,
                day + 3,
            ),
        ]);

        assert_eq!(aggregates.len(), 2);
        let traces = aggregates
            .iter()
            .find(|aggregate| aggregate.signal_kind == "traces")
            .unwrap();
        assert_eq!(traces.compressed_bytes, 25);
        assert_eq!(traces.original_bytes, 20);
        assert_eq!(traces.records, 7);
        assert_eq!(traces.file_count, 2);
        assert_eq!(traces.day_micros, day);
        assert_eq!(traces.updated_at, day + 2);
    }

    #[test]
    fn aggregate_query_daily_groups_by_tenant_project_signal_day() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let day = 3 * 86_400 * 1_000_000;
        let aggregates = aggregate_query_daily(&[
            query_sample(
                tenant_id,
                project_id,
                SignalKind::Metrics,
                10,
                Some(3),
                day + 1,
            ),
            query_sample(
                tenant_id,
                project_id,
                SignalKind::Metrics,
                15,
                None,
                day + 2,
            ),
            query_sample(tenant_id, project_id, SignalKind::Logs, 7, Some(1), day + 3),
        ]);

        assert_eq!(aggregates.len(), 2);
        let metrics = aggregates
            .iter()
            .find(|aggregate| aggregate.signal_kind == "metrics")
            .unwrap();
        assert_eq!(metrics.bytes_scanned, 25);
        assert_eq!(metrics.rows_scanned, 3);
        assert_eq!(metrics.query_count, 2);
        assert_eq!(metrics.day_micros, day);
        assert_eq!(metrics.updated_at, day + 2);
    }

    #[test]
    fn rollup_value_builders_do_not_insert_separators_inside_timestamp_expression() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let day = 86_400 * 1_000_000;
        let mut ingestion_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO ingestion_daily");
        let mut query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO query_usage_daily");
        let mut monthly_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO ingest_query_monthly");

        push_ingestion_daily_values(
            &mut ingestion_builder,
            &[IngestionDailyAggregate {
                tenant_id,
                project_id,
                signal_kind: "traces",
                day_micros: day,
                compressed_bytes: 1,
                original_bytes: 2,
                records: 3,
                file_count: 1,
                updated_at: day + 1,
            }],
        );
        push_query_daily_values(
            &mut query_builder,
            &[QueryDailyAggregate {
                tenant_id,
                project_id,
                signal_kind: "traces",
                day_micros: day,
                bytes_scanned: 1,
                rows_scanned: 2,
                query_count: 1,
                updated_at: day + 1,
            }],
        );
        push_monthly_usage_values(
            &mut monthly_builder,
            &[MonthlyUsageAggregate {
                tenant_id,
                project_id,
                signal_kind: "traces",
                operation: "ingest",
                period_start_micros: day,
                used_bytes: 1,
                updated_at: day + 1,
            }],
        );

        let ingestion_sql = ingestion_builder.sql();
        let query_sql = query_builder.sql();
        let monthly_sql = monthly_builder.sql();

        assert!(ingestion_sql.contains("VALUES ("));
        assert!(query_sql.contains("VALUES ("));
        assert!(monthly_sql.contains("VALUES ("));
        assert!(!ingestion_sql.contains("to_timestamp(,"));
        assert!(!query_sql.contains("to_timestamp(,"));
        assert!(!monthly_sql.contains("to_timestamp(,"));
    }

    #[test]
    fn aggregate_monthly_usage_groups_by_operation_and_month() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let may_1 = chrono::DateTime::parse_from_rfc3339("2024-05-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        let may_15 = chrono::DateTime::parse_from_rfc3339("2024-05-15T12:00:00Z")
            .unwrap()
            .timestamp_micros();
        let may_20 = chrono::DateTime::parse_from_rfc3339("2024-05-20T12:00:00Z")
            .unwrap()
            .timestamp_micros();

        let writes = aggregate_monthly_usage_for_writes(&[
            write_sample(
                tenant_id,
                project_id,
                SignalKind::Traces,
                10,
                Some(10),
                1,
                may_15,
            ),
            write_sample(
                tenant_id,
                project_id,
                SignalKind::Traces,
                15,
                Some(15),
                1,
                may_20,
            ),
        ]);
        let queries = aggregate_monthly_usage_for_queries(&[
            query_sample(tenant_id, project_id, SignalKind::Traces, 9, None, may_15),
            query_sample(tenant_id, project_id, SignalKind::Traces, 11, None, may_20),
        ]);

        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].operation, "ingest");
        assert_eq!(writes[0].period_start_micros, may_1);
        assert_eq!(writes[0].used_bytes, 25);
        assert_eq!(writes[0].updated_at, may_20);

        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].operation, "query");
        assert_eq!(queries[0].period_start_micros, may_1);
        assert_eq!(queries[0].used_bytes, 20);
        assert_eq!(queries[0].updated_at, may_20);
    }
}
