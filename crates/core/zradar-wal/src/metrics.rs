/// WAL Prometheus metrics (SPEC §7).
///
/// All metrics use the `zradar_wal_` prefix and are registered in a shared
/// `prometheus::Registry` that can be scraped via HTTP.
use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight metric counters for the WAL. Does not depend on an external
/// Prometheus crate — just atomic counters that the metrics endpoint reads.
///
/// This avoids pulling in `prometheus` as a hard dependency of the WAL crate.
/// The server-side `/metrics` handler formats these as Prometheus text.
#[derive(Debug)]
pub struct WalMetrics {
    /// Total records appended.
    pub append_total: AtomicU64,
    /// Total bytes appended.
    pub append_bytes_total: AtomicU64,
    /// Total fsync calls made by group-commit.
    pub fsync_total: AtomicU64,
    /// Active segment size in bytes.
    pub segment_bytes: AtomicU64,
    /// Lag: bytes between append highwater and flush checkpoint.
    pub lag_bytes: AtomicU64,
    /// Lag: estimated seconds of unflushed data.
    pub lag_seconds: AtomicU64,
    /// Free bytes on the WAL filesystem.
    pub dir_free_bytes: AtomicU64,
    /// Current backpressure state (0=Normal, 1=Slowdown, 2=Reject).
    pub backpressure_state: AtomicU64,
    /// Records replayed on last startup.
    pub replay_records_total: AtomicU64,
    /// Duration of last replay in milliseconds.
    pub replay_duration_ms: AtomicU64,
    /// Total torn writes detected and recovered.
    pub torn_writes_total: AtomicU64,
    /// Append duration histogram bucket (p99 approximation via exponential decay).
    pub append_duration_ns_sum: AtomicU64,
    pub append_duration_ns_count: AtomicU64,
}

impl WalMetrics {
    pub fn new() -> Self {
        Self {
            append_total: AtomicU64::new(0),
            append_bytes_total: AtomicU64::new(0),
            fsync_total: AtomicU64::new(0),
            segment_bytes: AtomicU64::new(0),
            lag_bytes: AtomicU64::new(0),
            lag_seconds: AtomicU64::new(0),
            dir_free_bytes: AtomicU64::new(0),
            backpressure_state: AtomicU64::new(0),
            replay_records_total: AtomicU64::new(0),
            replay_duration_ms: AtomicU64::new(0),
            torn_writes_total: AtomicU64::new(0),
            append_duration_ns_sum: AtomicU64::new(0),
            append_duration_ns_count: AtomicU64::new(0),
        }
    }

    /// Record an append operation.
    pub fn record_append(&self, bytes: u64, duration_ns: u64) {
        self.append_total.fetch_add(1, Ordering::Relaxed);
        self.append_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.append_duration_ns_sum
            .fetch_add(duration_ns, Ordering::Relaxed);
        self.append_duration_ns_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an fsync.
    pub fn record_fsync(&self) {
        self.fsync_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a torn write recovery.
    pub fn record_torn_write(&self) {
        self.torn_writes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        write_counter(
            &mut out,
            "zradar_wal_append_total",
            "Total WAL records appended",
            self.append_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_wal_append_bytes_total",
            "Total bytes appended to WAL",
            self.append_bytes_total.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_wal_fsync_total",
            "Total fsync calls by group-commit",
            self.fsync_total.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_segment_bytes",
            "Active segment size in bytes",
            self.segment_bytes.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_lag_bytes",
            "Bytes between append highwater and flush checkpoint",
            self.lag_bytes.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_lag_seconds",
            "Estimated seconds of unflushed data",
            self.lag_seconds.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_dir_free_bytes",
            "Free bytes on WAL filesystem",
            self.dir_free_bytes.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_backpressure_state",
            "Backpressure state (0=Normal, 1=Slowdown, 2=Reject)",
            self.backpressure_state.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "zradar_wal_replay_records_total",
            "Records replayed on last startup",
            self.replay_records_total.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "zradar_wal_replay_duration_seconds",
            "Duration of last replay",
            self.replay_duration_ms.load(Ordering::Relaxed) / 1000,
        );
        write_counter(
            &mut out,
            "zradar_wal_torn_writes_total",
            "Total torn writes detected and recovered",
            self.torn_writes_total.load(Ordering::Relaxed),
        );

        // Append duration summary
        let count = self.append_duration_ns_count.load(Ordering::Relaxed);
        let sum_ns = self.append_duration_ns_sum.load(Ordering::Relaxed);
        out.push_str("# HELP zradar_wal_append_duration_seconds Append latency\n");
        out.push_str("# TYPE zradar_wal_append_duration_seconds summary\n");
        out.push_str(&format!(
            "zradar_wal_append_duration_seconds_sum {}\n",
            sum_ns as f64 / 1_000_000_000.0
        ));
        out.push_str(&format!(
            "zradar_wal_append_duration_seconds_count {count}\n"
        ));

        out
    }
}

impl Default for WalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} counter\n"));
    out.push_str(&format!("{name} {value}\n"));
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} gauge\n"));
    out.push_str(&format!("{name} {value}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_render_prometheus() {
        let m = WalMetrics::new();
        m.record_append(1024, 500_000);
        m.record_append(2048, 1_000_000);
        m.record_fsync();
        m.record_torn_write();

        let output = m.render_prometheus();
        assert!(output.contains("zradar_wal_append_total 2"));
        assert!(output.contains("zradar_wal_append_bytes_total 3072"));
        assert!(output.contains("zradar_wal_fsync_total 1"));
        assert!(output.contains("zradar_wal_torn_writes_total 1"));
        assert!(output.contains("zradar_wal_append_duration_seconds_count 2"));
    }

    #[test]
    fn test_metrics_default_zero() {
        let m = WalMetrics::new();
        let output = m.render_prometheus();
        assert!(output.contains("zradar_wal_append_total 0"));
        assert!(output.contains("zradar_wal_lag_bytes 0"));
    }
}
