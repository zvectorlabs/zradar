use crate::client::PostgresClient;
use async_trait::async_trait;
use sqlx::{Postgres, QueryBuilder, Row};
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

        loop {
            tokio::select! {
                Some(event) = self.receiver.recv() => {
                    match event {
                        UsageEvent::Write(sample) => writes.push(sample),
                        UsageEvent::Query(sample) => queries.push(sample),
                    }
                    self.record_buffered_samples(writes.len(), queries.len());
                    self.enforce_retry_buffer_limit(&mut writes, &mut queries);

                    if writes.len() + queries.len() >= self.batch_size {
                        self.flush(&mut writes, &mut queries).await;
                    }
                }
                _ = tick.tick() => {
                    self.flush(&mut writes, &mut queries).await;
                }
                else => {
                    self.flush(&mut writes, &mut queries).await;
                    return;
                }
            }
        }
    }

    async fn flush(&self, writes: &mut Vec<WriteSample>, queries: &mut Vec<QuerySample>) {
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
                warn!(error = %e, count, "failed to flush usage write samples");
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
            }
        }

        if !queries.is_empty() {
            let count = queries.len();
            let started_at = Instant::now();
            if let Err(e) = insert_query_samples(self.client.as_ref(), queries.as_slice()).await {
                self.metrics
                    .flush_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, count, "failed to flush usage query samples");
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
        while writes.len().saturating_add(queries.len()) > self.retry_buffer_max_samples {
            if writes.len() >= queries.len() && !writes.is_empty() {
                writes.remove(0);
                self.metrics
                    .write_samples_dropped_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if !queries.is_empty() {
                queries.remove(0);
                self.metrics
                    .query_samples_dropped_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                break;
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
        .bind(event.period_start.unwrap_or(0))
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

async fn upsert_ingestion_daily(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[WriteSample],
) -> Result<(), sqlx::Error> {
    for sample in samples {
        sqlx::query(
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
            ) VALUES (
                $1,
                $2,
                $3,
                (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date,
                $5,
                $6,
                $7,
                $8,
                $9
            )
            ON CONFLICT (tenant_id, project_id, signal_kind, day)
            DO UPDATE SET
                compressed_bytes = ingestion_daily.compressed_bytes + EXCLUDED.compressed_bytes,
                original_bytes = ingestion_daily.original_bytes + EXCLUDED.original_bytes,
                records = ingestion_daily.records + EXCLUDED.records,
                file_count = ingestion_daily.file_count + EXCLUDED.file_count,
                updated_at = GREATEST(ingestion_daily.updated_at, EXCLUDED.updated_at)
            "#,
        )
        .bind(sample.tenant_id)
        .bind(sample.project_id)
        .bind(signal_kind(sample.signal))
        .bind(sample.flushed_at)
        .bind(sample.compressed_bytes)
        .bind(sample.original_bytes.unwrap_or(0))
        .bind(sample.records)
        .bind(1_i64)
        .bind(sample.flushed_at)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn upsert_query_usage_daily(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[QuerySample],
) -> Result<(), sqlx::Error> {
    for sample in samples {
        sqlx::query(
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
            ) VALUES (
                $1,
                $2,
                $3,
                (to_timestamp($4::double precision / 1000000.0) AT TIME ZONE 'UTC')::date,
                $5,
                $6,
                $7,
                $8
            )
            ON CONFLICT (tenant_id, project_id, signal_kind, day)
            DO UPDATE SET
                bytes_scanned = query_usage_daily.bytes_scanned + EXCLUDED.bytes_scanned,
                rows_scanned = query_usage_daily.rows_scanned + EXCLUDED.rows_scanned,
                query_count = query_usage_daily.query_count + EXCLUDED.query_count,
                updated_at = GREATEST(query_usage_daily.updated_at, EXCLUDED.updated_at)
            "#,
        )
        .bind(sample.tenant_id)
        .bind(sample.project_id)
        .bind(signal_kind(sample.signal))
        .bind(sample.submitted_at)
        .bind(sample.bytes_scanned)
        .bind(sample.rows_scanned.unwrap_or(0))
        .bind(1_i64)
        .bind(sample.submitted_at)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn upsert_ingest_query_monthly_for_writes(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[WriteSample],
) -> Result<(), sqlx::Error> {
    for sample in samples {
        upsert_monthly_usage(
            tx,
            sample.tenant_id,
            sample.project_id,
            sample.signal,
            Operation::Ingest,
            sample.flushed_at,
            sample.compressed_bytes,
        )
        .await?;
    }

    Ok(())
}

async fn upsert_ingest_query_monthly_for_queries(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    samples: &[QuerySample],
) -> Result<(), sqlx::Error> {
    for sample in samples {
        upsert_monthly_usage(
            tx,
            sample.tenant_id,
            sample.project_id,
            sample.signal,
            Operation::Query,
            sample.submitted_at,
            sample.bytes_scanned,
        )
        .await?;
    }

    Ok(())
}

async fn upsert_monthly_usage(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
    operation: Operation,
    observed_at: i64,
    used_bytes: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
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
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            date_trunc(
                'month',
                to_timestamp($5::double precision / 1000000.0) AT TIME ZONE 'UTC'
            )::date,
            $6,
            $7,
            NULL,
            $8
        )
        ON CONFLICT (tenant_id, project_id, signal_kind, operation, period_start)
        DO UPDATE SET
            used_bytes = ingest_query_monthly.used_bytes + EXCLUDED.used_bytes,
            updated_at = GREATEST(ingest_query_monthly.updated_at, EXCLUDED.updated_at)
        "#,
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(signal_kind(signal))
    .bind(operation_kind(operation))
    .bind(observed_at)
    .bind(used_bytes)
    .bind(0_i64)
    .bind(observed_at)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

fn operation_kind(operation: Operation) -> &'static str {
    match operation {
        Operation::Ingest => "ingest",
        Operation::Query => "query",
        Operation::Store => "store",
        Operation::All => "all",
    }
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
}
