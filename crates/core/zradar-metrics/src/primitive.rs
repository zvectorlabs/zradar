//! Lock-free metric primitives and a Prometheus text encoder.
//!
//! These deliberately avoid the `prometheus` crate (matching the convention in
//! `zradar_wal::metrics`): every metric is a plain atomic, and the `/metrics`
//! handler renders them as Prometheus exposition text via [`PromEncoder`].
//!
//! Extending: add a [`Counter`] / [`Gauge`] / [`Histogram`] field to a metric
//! group, then emit it in that group's `collect` with one `enc.counter(..)` /
//! `enc.gauge(..)` / `enc.histogram(..)` call.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// A monotonically increasing counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }
    #[inline]
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Relaxed);
    }
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// An instantaneous value that can move up or down. Stored as `f64` bits so it
/// can hold both counts (`set_u64`) and ratios (`set`).
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    #[inline]
    pub fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Relaxed);
    }
    #[inline]
    pub fn set_u64(&self, v: u64) {
        self.set(v as f64);
    }
    #[inline]
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Relaxed))
    }
}

/// Upper bounds (`le`) for latency histograms, in **seconds**.
pub const LATENCY_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Upper bounds (`le`) for size histograms, in **bytes**.
pub const SIZE_BYTES: &[f64] = &[
    1_024.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
];

/// A cumulative-bucket histogram over atomics (Prometheus histogram shape).
///
/// `bounds` are the upper bounds in the metric's base unit; an implicit `+Inf`
/// bucket is appended. `observe` is O(bounds) with a small fixed bound, so it is
/// cheap on the hot path.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    counts: Vec<AtomicU64>, // len == bounds.len() + 1 (last == +Inf)
    sum_bits: AtomicU64,    // f64 bits — sum of observations in the base unit
    count: AtomicU64,
}

impl Histogram {
    #[must_use]
    pub fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_bits: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Observe a duration (rendered in seconds).
    #[inline]
    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_secs_f64());
    }

    /// Observe a raw value in the histogram's base unit.
    pub fn observe(&self, v: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|&b| v <= b)
            .unwrap_or(self.bounds.len());
        self.counts[idx].fetch_add(1, Relaxed);
        self.count.fetch_add(1, Relaxed);
        // Atomic f64 add via CAS — contention here is negligible vs the work
        // that produced the observation.
        let mut cur = self.sum_bits.load(Relaxed);
        loop {
            let next = (f64::from_bits(cur) + v).to_bits();
            match self
                .sum_bits
                .compare_exchange_weak(cur, next, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Relaxed))
    }
}

/// Accumulates Prometheus exposition text. `# HELP`/`# TYPE` lines are emitted
/// once per metric name (the first sample), so the same metric can be rendered
/// repeatedly with different label sets (e.g. once per observed workspace).
#[derive(Debug, Default)]
pub struct PromEncoder {
    out: String,
    seen: HashSet<&'static str>,
}

impl PromEncoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.out
    }

    fn meta(&mut self, name: &'static str, help: &str, typ: &str) {
        if self.seen.insert(name) {
            self.out.push_str("# HELP ");
            self.out.push_str(name);
            self.out.push(' ');
            self.out.push_str(help);
            self.out.push('\n');
            self.out.push_str("# TYPE ");
            self.out.push_str(name);
            self.out.push(' ');
            self.out.push_str(typ);
            self.out.push('\n');
        }
    }

    fn write_labels(&mut self, labels: &[(&str, &str)]) {
        if labels.is_empty() {
            return;
        }
        self.out.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                self.out.push(',');
            }
            self.out.push_str(k);
            self.out.push_str("=\"");
            for c in v.chars() {
                match c {
                    '"' => self.out.push_str("\\\""),
                    '\\' => self.out.push_str("\\\\"),
                    '\n' => self.out.push_str("\\n"),
                    _ => self.out.push(c),
                }
            }
            self.out.push('"');
        }
        self.out.push('}');
    }

    fn line_u64(&mut self, name: &str, labels: &[(&str, &str)], value: u64) {
        self.out.push_str(name);
        self.write_labels(labels);
        self.out.push(' ');
        self.out.push_str(itoa(value).as_str());
        self.out.push('\n');
    }

    fn line_f64(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.out.push_str(name);
        self.write_labels(labels);
        self.out.push(' ');
        self.out.push_str(&ftoa(value));
        self.out.push('\n');
    }

    /// Emit a counter sample.
    pub fn counter(&mut self, name: &'static str, help: &str, value: u64, labels: &[(&str, &str)]) {
        self.meta(name, help, "counter");
        self.line_u64(name, labels, value);
    }

    /// Emit a gauge sample.
    pub fn gauge(&mut self, name: &'static str, help: &str, value: f64, labels: &[(&str, &str)]) {
        self.meta(name, help, "gauge");
        self.line_f64(name, labels, value);
    }

    /// Emit a histogram's `_bucket`/`_sum`/`_count` samples.
    pub fn histogram(
        &mut self,
        name: &'static str,
        help: &str,
        h: &Histogram,
        labels: &[(&str, &str)],
    ) {
        self.meta(name, help, "histogram");
        let bucket_name = format!("{name}_bucket");
        let mut cumulative = 0u64;
        for (i, bound) in h.bounds.iter().enumerate() {
            cumulative += h.counts[i].load(Relaxed);
            let le = ftoa(*bound);
            let mut lbls: Vec<(&str, &str)> = labels.to_vec();
            lbls.push(("le", &le));
            self.line_u64(&bucket_name, &lbls, cumulative);
        }
        cumulative += h.counts[h.bounds.len()].load(Relaxed);
        let mut inf = labels.to_vec();
        inf.push(("le", "+Inf"));
        self.line_u64(&bucket_name, &inf, cumulative);
        self.line_f64(&format!("{name}_sum"), labels, h.sum());
        self.line_u64(&format!("{name}_count"), labels, cumulative);
    }
}

fn itoa(v: u64) -> String {
    v.to_string()
}

/// Format an f64 for Prometheus exposition (finite → default fmt; non-finite →
/// the Prometheus spellings).
fn ftoa(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_and_gauge() {
        let c = Counter::default();
        c.inc();
        c.add(4);
        assert_eq!(c.get(), 5);
        let g = Gauge::default();
        g.set_u64(7);
        assert_eq!(g.get(), 7.0);
        g.set(0.5);
        assert_eq!(g.get(), 0.5);
    }

    #[test]
    fn histogram_buckets_are_cumulative() {
        let h = Histogram::new(LATENCY_SECONDS);
        h.observe(0.003); // falls in le=0.005
        h.observe(0.2); // falls in le=0.25
        h.observe(100.0); // +Inf
        let mut enc = PromEncoder::new();
        enc.histogram("t_seconds", "help", &h, &[("signal", "traces")]);
        let out = enc.into_string();
        assert!(out.contains("# TYPE t_seconds histogram"));
        // le=0.005 has 1 (the 0.003), le=0.25 has 2 (cumulative), +Inf has 3.
        assert!(out.contains("t_seconds_bucket{signal=\"traces\",le=\"0.005\"} 1"));
        assert!(out.contains("t_seconds_bucket{signal=\"traces\",le=\"0.25\"} 2"));
        assert!(out.contains("t_seconds_bucket{signal=\"traces\",le=\"+Inf\"} 3"));
        assert!(out.contains("t_seconds_count{signal=\"traces\"} 3"));
    }

    #[test]
    fn help_type_emitted_once_per_name() {
        let mut enc = PromEncoder::new();
        enc.counter("c_total", "h", 1, &[("workspace", "a")]);
        enc.counter("c_total", "h", 2, &[("workspace", "b")]);
        let out = enc.into_string();
        assert_eq!(out.matches("# TYPE c_total counter").count(), 1);
        assert!(out.contains("c_total{workspace=\"a\"} 1"));
        assert!(out.contains("c_total{workspace=\"b\"} 2"));
    }

    #[test]
    fn label_values_are_escaped() {
        let mut enc = PromEncoder::new();
        enc.gauge("g", "h", 1.0, &[("k", "a\"b\\c")]);
        assert!(enc.into_string().contains(r#"g{k="a\"b\\c"} 1"#));
    }
}
