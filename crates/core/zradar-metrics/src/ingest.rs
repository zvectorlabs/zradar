//! Ingest/storage metric groups.
//!
//! [`IngestMetrics`] is the always-on, system-wide set (low, fixed
//! cardinality). [`WorkspaceMetrics`] is the per-workspace subset recorded only
//! for policy-"observed" workspaces (see [`crate::MetricsHub`]); it uses
//! distinct `_ws_` metric names so per-tenant series never double-count the
//! system aggregates.
//!
//! Extending with a new metric:
//! 1. add a `Counter`/`Gauge`/`Histogram` field to the group,
//! 2. initialise it in `new`,
//! 3. emit it in `collect` with one `enc.*` call,
//! 4. add a `record_*`/setter on [`crate::MetricsHub`].

use crate::primitive::{Counter, Gauge, Histogram, LATENCY_SECONDS, PromEncoder, SIZE_BYTES};

/// Telemetry signal kind — a bounded metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
    Scores,
}

impl Signal {
    pub const ALL: [Signal; 4] = [
        Signal::Traces,
        Signal::Metrics,
        Signal::Logs,
        Signal::Scores,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
            Signal::Scores => "scores",
        }
    }
}

/// Why a request/row was rejected by the guard chain — a bounded metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    Auth,
    CircuitBreaker,
    Policy,
    RateLimit,
    ParserCap,
    Internal,
}

impl RejectReason {
    pub const ALL: [RejectReason; 6] = [
        RejectReason::Auth,
        RejectReason::CircuitBreaker,
        RejectReason::Policy,
        RejectReason::RateLimit,
        RejectReason::ParserCap,
        RejectReason::Internal,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RejectReason::Auth => "auth",
            RejectReason::CircuitBreaker => "circuit_breaker",
            RejectReason::Policy => "policy",
            RejectReason::RateLimit => "rate_limit",
            RejectReason::ParserCap => "parser_cap",
            RejectReason::Internal => "internal",
        }
    }
}

/// What pushed the circuit breaker open — a bounded metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerCause {
    Disk,
    Memory,
    Queue,
}

impl BreakerCause {
    pub const ALL: [BreakerCause; 3] = [
        BreakerCause::Disk,
        BreakerCause::Memory,
        BreakerCause::Queue,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            BreakerCause::Disk => "disk",
            BreakerCause::Memory => "memory",
            BreakerCause::Queue => "queue",
        }
    }
}

fn counters(n: usize) -> Vec<Counter> {
    (0..n).map(|_| Counter::default()).collect()
}

fn hists(n: usize, bounds: &'static [f64]) -> Vec<Histogram> {
    (0..n).map(|_| Histogram::new(bounds)).collect()
}

/// Always-on, system-wide ingest/storage metrics.
#[derive(Debug)]
pub struct IngestMetrics {
    // ---- ack path (client-facing) ----
    requests: Vec<Counter>,          // [Signal]
    rejections: Vec<Counter>,        // [RejectReason]
    rows_accepted: Vec<Counter>,     // [Signal]
    bytes_ingested: Vec<Counter>,    // [Signal]
    ack_latency: Vec<Histogram>,     // [Signal]
    convert_latency: Vec<Histogram>, // [Signal]
    // ---- freshness ----
    visibility_lag: Vec<Histogram>, // [Signal]
    // ---- write / storage ----
    flush_records: Vec<Counter>, // [Signal]
    parquet_files: Vec<Counter>, // [Signal]
    parquet_file_bytes: Histogram,
    buffer_records: Gauge,
    buffer_bytes: Gauge,
    // ---- saturation ----
    queue_depth: Gauge,
    cb_state: Gauge,
    cb_trips: Vec<Counter>, // [BreakerCause]
    disk_usage_ratio: Gauge,
    // ---- correctness ----
    convert_skipped: Counter,
    timestamp_anomalies: Counter,
    dropped: Counter,
}

impl Default for IngestMetrics {
    fn default() -> Self {
        Self {
            requests: counters(Signal::ALL.len()),
            rejections: counters(RejectReason::ALL.len()),
            rows_accepted: counters(Signal::ALL.len()),
            bytes_ingested: counters(Signal::ALL.len()),
            ack_latency: hists(Signal::ALL.len(), LATENCY_SECONDS),
            convert_latency: hists(Signal::ALL.len(), LATENCY_SECONDS),
            visibility_lag: hists(Signal::ALL.len(), LATENCY_SECONDS),
            flush_records: counters(Signal::ALL.len()),
            parquet_files: counters(Signal::ALL.len()),
            parquet_file_bytes: Histogram::new(SIZE_BYTES),
            buffer_records: Gauge::default(),
            buffer_bytes: Gauge::default(),
            queue_depth: Gauge::default(),
            cb_state: Gauge::default(),
            cb_trips: counters(BreakerCause::ALL.len()),
            disk_usage_ratio: Gauge::default(),
            convert_skipped: Counter::default(),
            timestamp_anomalies: Counter::default(),
            dropped: Counter::default(),
        }
    }
}

impl IngestMetrics {
    // --- recording (called via MetricsHub) ---
    pub(crate) fn request(&self, s: Signal) {
        self.requests[s as usize].inc();
    }
    pub(crate) fn rejected(&self, r: RejectReason) {
        self.rejections[r as usize].inc();
    }
    pub(crate) fn accepted(&self, s: Signal, rows: u64, bytes: u64) {
        self.rows_accepted[s as usize].add(rows);
        self.bytes_ingested[s as usize].add(bytes);
    }
    pub(crate) fn observe_ack(&self, s: Signal, secs: f64) {
        self.ack_latency[s as usize].observe(secs);
    }
    pub(crate) fn observe_convert(&self, s: Signal, secs: f64) {
        self.convert_latency[s as usize].observe(secs);
    }
    pub(crate) fn observe_visibility(&self, s: Signal, secs: f64) {
        self.visibility_lag[s as usize].observe(secs);
    }
    pub(crate) fn flushed(&self, s: Signal, rows: u64) {
        self.flush_records[s as usize].add(rows);
    }
    pub(crate) fn parquet_written(&self, s: Signal, bytes: u64) {
        self.parquet_files[s as usize].inc();
        self.parquet_file_bytes.observe(bytes as f64);
    }
    pub(crate) fn set_buffer(&self, rows: u64, bytes: u64) {
        self.buffer_records.set_u64(rows);
        self.buffer_bytes.set_u64(bytes);
    }
    pub(crate) fn set_queue_depth(&self, n: u64) {
        self.queue_depth.set_u64(n);
    }
    pub(crate) fn set_breaker(&self, open: bool) {
        self.cb_state.set(if open { 1.0 } else { 0.0 });
    }
    pub(crate) fn breaker_trip(&self, c: BreakerCause) {
        self.cb_trips[c as usize].inc();
    }
    pub(crate) fn set_disk_ratio(&self, ratio: f64) {
        self.disk_usage_ratio.set(ratio);
    }
    pub(crate) fn convert_skipped(&self) {
        self.convert_skipped.inc();
    }
    pub(crate) fn timestamp_anomaly(&self) {
        self.timestamp_anomalies.inc();
    }
    pub(crate) fn dropped(&self, n: u64) {
        self.dropped.add(n);
    }

    /// Render all system metrics into the encoder.
    pub fn collect(&self, enc: &mut PromEncoder) {
        for s in Signal::ALL {
            let i = s as usize;
            let l = &[("signal", s.as_str())];
            enc.counter(
                "zradar_ingest_requests_total",
                "OTLP export requests received.",
                self.requests[i].get(),
                l,
            );
            enc.counter(
                "zradar_ingest_rows_accepted_total",
                "Telemetry rows accepted after the guard chain.",
                self.rows_accepted[i].get(),
                l,
            );
            enc.counter(
                "zradar_ingest_bytes_total",
                "Telemetry bytes accepted.",
                self.bytes_ingested[i].get(),
                l,
            );
            enc.histogram(
                "zradar_ingest_ack_latency_seconds",
                "Receive→ack latency (includes WAL fsync).",
                &self.ack_latency[i],
                l,
            );
            enc.histogram(
                "zradar_ingest_convert_duration_seconds",
                "OTLP→domain conversion latency.",
                &self.convert_latency[i],
                l,
            );
            enc.histogram(
                "zradar_ingest_visibility_lag_seconds",
                "Ack→queryable lag (async WAL flush).",
                &self.visibility_lag[i],
                l,
            );
            enc.counter(
                "zradar_storage_flush_records_total",
                "Records flushed from WAL to Parquet.",
                self.flush_records[i].get(),
                l,
            );
            enc.counter(
                "zradar_storage_parquet_files_total",
                "Parquet files written.",
                self.parquet_files[i].get(),
                l,
            );
        }
        for r in RejectReason::ALL {
            enc.counter(
                "zradar_ingest_rejections_total",
                "Requests/rows rejected by the guard chain.",
                self.rejections[r as usize].get(),
                &[("reason", r.as_str())],
            );
        }
        for c in BreakerCause::ALL {
            enc.counter(
                "zradar_saturation_circuit_breaker_trips_total",
                "Circuit-breaker trips by cause.",
                self.cb_trips[c as usize].get(),
                &[("cause", c.as_str())],
            );
        }
        enc.histogram(
            "zradar_storage_parquet_file_bytes",
            "Compressed Parquet file size distribution.",
            &self.parquet_file_bytes,
            &[],
        );
        enc.gauge(
            "zradar_storage_buffer_records",
            "Rows buffered awaiting flush.",
            self.buffer_records.get(),
            &[],
        );
        enc.gauge(
            "zradar_storage_buffer_bytes",
            "Bytes buffered awaiting flush.",
            self.buffer_bytes.get(),
            &[],
        );
        enc.gauge(
            "zradar_saturation_queue_depth",
            "Pending write-buffer record count.",
            self.queue_depth.get(),
            &[],
        );
        enc.gauge(
            "zradar_saturation_circuit_breaker_state",
            "Circuit-breaker state (0=closed, 1=open).",
            self.cb_state.get(),
            &[],
        );
        enc.gauge(
            "zradar_saturation_disk_usage_ratio",
            "Parquet data dir disk usage ratio (0..1).",
            self.disk_usage_ratio.get(),
            &[],
        );
        enc.counter(
            "zradar_correctness_convert_skipped_total",
            "Malformed records skipped during conversion.",
            self.convert_skipped.get(),
            &[],
        );
        enc.counter(
            "zradar_correctness_timestamp_anomalies_total",
            "Spans with anomalous timestamps (end<start, future, zero-duration).",
            self.timestamp_anomalies.get(),
            &[],
        );
        enc.counter(
            "zradar_correctness_dropped_total",
            "Unexpected drops between accept and persist (data-loss signal).",
            self.dropped.get(),
            &[],
        );
    }
}

/// Per-workspace subset, recorded only for policy-observed workspaces. Distinct
/// `_ws_` names keep these from double-counting the system aggregates.
#[derive(Debug)]
pub struct WorkspaceMetrics {
    rows_accepted: Vec<Counter>,    // [Signal]
    bytes_ingested: Vec<Counter>,   // [Signal]
    rejections: Vec<Counter>,       // [RejectReason]
    ack_latency: Vec<Histogram>,    // [Signal]
    visibility_lag: Vec<Histogram>, // [Signal]
}

impl Default for WorkspaceMetrics {
    fn default() -> Self {
        Self {
            rows_accepted: counters(Signal::ALL.len()),
            bytes_ingested: counters(Signal::ALL.len()),
            rejections: counters(RejectReason::ALL.len()),
            ack_latency: hists(Signal::ALL.len(), LATENCY_SECONDS),
            visibility_lag: hists(Signal::ALL.len(), LATENCY_SECONDS),
        }
    }
}

impl WorkspaceMetrics {
    pub(crate) fn accepted(&self, s: Signal, rows: u64, bytes: u64) {
        self.rows_accepted[s as usize].add(rows);
        self.bytes_ingested[s as usize].add(bytes);
    }
    pub(crate) fn rejected(&self, r: RejectReason) {
        self.rejections[r as usize].inc();
    }
    pub(crate) fn observe_ack(&self, s: Signal, secs: f64) {
        self.ack_latency[s as usize].observe(secs);
    }
    pub(crate) fn observe_visibility(&self, s: Signal, secs: f64) {
        self.visibility_lag[s as usize].observe(secs);
    }

    /// Render this workspace's metrics, labelled with `workspace`.
    pub fn collect(&self, enc: &mut PromEncoder, workspace: &str) {
        for s in Signal::ALL {
            let i = s as usize;
            let l = &[("workspace", workspace), ("signal", s.as_str())];
            enc.counter(
                "zradar_ingest_ws_rows_accepted_total",
                "Rows accepted, per observed workspace.",
                self.rows_accepted[i].get(),
                l,
            );
            enc.counter(
                "zradar_ingest_ws_bytes_total",
                "Bytes accepted, per observed workspace.",
                self.bytes_ingested[i].get(),
                l,
            );
            enc.histogram(
                "zradar_ingest_ws_ack_latency_seconds",
                "Ack latency, per observed workspace.",
                &self.ack_latency[i],
                l,
            );
            enc.histogram(
                "zradar_ingest_ws_visibility_lag_seconds",
                "Ack→queryable lag, per observed workspace.",
                &self.visibility_lag[i],
                l,
            );
        }
        for r in RejectReason::ALL {
            enc.counter(
                "zradar_ingest_ws_rejections_total",
                "Rejections, per observed workspace.",
                self.rejections[r as usize].get(),
                &[("workspace", workspace), ("reason", r.as_str())],
            );
        }
    }
}
