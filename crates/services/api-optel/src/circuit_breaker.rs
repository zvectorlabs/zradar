//! Ingestion circuit breaker.
//!
//! Rejects OTLP exports while the node is under resource pressure: disk usage
//! of the data directory, memory usage of the process, or the write-buffer
//! queue depth exceed their configured thresholds.
//!
//! Disk and memory usage are measured in-process (`statvfs(2)` and `/proc` /
//! cgroup files) so the breaker works in minimal container images that ship no
//! shell utilities. Samples are taken on a blocking thread and cached for
//! [`RESOURCE_SAMPLE_INTERVAL`] so the per-request cost is a few atomic loads.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tonic::Status;
use tracing::warn;

/// How long a disk/memory sample stays valid before it is refreshed.
pub const RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Sentinel stored in the usage atomics when a measurement is unavailable.
const UNKNOWN_PERCENT: u8 = u8::MAX;
/// Sentinel for "never sampled".
const NEVER_SAMPLED: u64 = u64::MAX;
/// cgroup v1 reports "no limit" as a huge page-aligned number; treat anything
/// at or above this as unlimited.
const CGROUP_UNLIMITED_THRESHOLD: u64 = 1 << 60;

#[derive(Debug)]
pub struct CircuitBreaker {
    data_dir: PathBuf,
    max_disk_usage_percent: u8,
    max_memory_usage_percent: u8,
    max_queue_depth: u64,
    queue_depth: AtomicU64,
    created_at: Instant,
    /// Milliseconds since `created_at` of the last resource sample.
    last_sample_ms: AtomicU64,
    sampling: AtomicBool,
    disk_usage_percent: AtomicU8,
    memory_usage_percent: AtomicU8,
    warned_unavailable: AtomicBool,
}

impl CircuitBreaker {
    pub fn new(
        data_dir: PathBuf,
        max_disk_usage_percent: u8,
        max_memory_usage_percent: u8,
        max_queue_depth: u64,
    ) -> Self {
        Self {
            data_dir,
            max_disk_usage_percent,
            max_memory_usage_percent,
            max_queue_depth,
            queue_depth: AtomicU64::new(0),
            created_at: Instant::now(),
            last_sample_ms: AtomicU64::new(NEVER_SAMPLED),
            sampling: AtomicBool::new(false),
            disk_usage_percent: AtomicU8::new(UNKNOWN_PERCENT),
            memory_usage_percent: AtomicU8::new(UNKNOWN_PERCENT),
            warned_unavailable: AtomicBool::new(false),
        }
    }

    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub fn max_queue_depth(&self) -> u64 {
        self.max_queue_depth
    }

    /// Last sampled disk usage (percent) of the data directory's filesystem.
    pub fn disk_usage_percent(&self) -> Option<u8> {
        known(self.disk_usage_percent.load(Ordering::Relaxed))
    }

    /// Last sampled memory usage (percent of the effective memory limit).
    pub fn memory_usage_percent(&self) -> Option<u8> {
        known(self.memory_usage_percent.load(Ordering::Relaxed))
    }

    /// Check all thresholds. Disk and memory checks fail open when the
    /// measurement is unavailable (e.g. on non-Linux hosts); a warning is
    /// logged once in that case.
    pub async fn check(&self) -> Result<(), String> {
        self.refresh_resources_if_stale().await;

        if let Some(disk_usage) = self.disk_usage_percent()
            && disk_usage > self.max_disk_usage_percent
        {
            return Err(format!("disk usage {}% exceeds threshold", disk_usage));
        }

        if let Some(memory_usage) = self.memory_usage_percent()
            && memory_usage > self.max_memory_usage_percent
        {
            return Err(format!("memory usage {}% exceeds threshold", memory_usage));
        }

        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        if queue_depth > self.max_queue_depth {
            return Err(format!("queue depth {} exceeds threshold", queue_depth));
        }

        Ok(())
    }

    /// [`check`](Self::check) as a gRPC status.
    ///
    /// A tripped breaker is transient overload, so it maps to `UNAVAILABLE`
    /// with a `RetryInfo` delay of one sample interval. OTLP exporters retry
    /// `UNAVAILABLE`; they treat `RESOURCE_EXHAUSTED` without `RetryInfo` as
    /// permanent and drop the data
    /// (<https://opentelemetry.io/docs/specs/otlp/#failures>).
    pub async fn check_status(&self) -> Result<(), Status> {
        self.check().await.map_err(|reason| {
            crate::retry::unavailable(
                format!("Ingestion backpressure: {}", reason),
                RESOURCE_SAMPLE_INTERVAL,
            )
        })
    }

    /// Re-sample disk and memory usage if the cached sample is older than
    /// [`RESOURCE_SAMPLE_INTERVAL`]. Only one caller samples at a time; other
    /// callers keep using the cached values.
    async fn refresh_resources_if_stale(&self) {
        let now_ms = self.elapsed_ms();
        let last = self.last_sample_ms.load(Ordering::Acquire);
        let interval_ms = RESOURCE_SAMPLE_INTERVAL.as_millis() as u64;
        if last != NEVER_SAMPLED && now_ms.saturating_sub(last) < interval_ms {
            return;
        }
        if self
            .sampling
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let dir = self.data_dir.clone();
        let sample = tokio::task::spawn_blocking(move || ResourceSample::collect(&dir))
            .await
            .unwrap_or_default();

        self.disk_usage_percent.store(
            sample.disk_percent.unwrap_or(UNKNOWN_PERCENT),
            Ordering::Relaxed,
        );
        self.memory_usage_percent.store(
            sample.memory_percent.unwrap_or(UNKNOWN_PERCENT),
            Ordering::Relaxed,
        );
        if (sample.disk_percent.is_none() || sample.memory_percent.is_none())
            && !self.warned_unavailable.swap(true, Ordering::Relaxed)
        {
            warn!(
                data_dir = %self.data_dir.display(),
                disk_available = sample.disk_percent.is_some(),
                memory_available = sample.memory_percent.is_some(),
                "circuit breaker: resource usage unavailable; affected checks fail open"
            );
        }
        self.last_sample_ms
            .store(self.elapsed_ms(), Ordering::Release);
        self.sampling.store(false, Ordering::Release);
    }

    fn elapsed_ms(&self) -> u64 {
        // Never collide with the NEVER_SAMPLED sentinel.
        (self.created_at.elapsed().as_millis() as u64).min(NEVER_SAMPLED - 1)
    }
}

fn known(value: u8) -> Option<u8> {
    (value != UNKNOWN_PERCENT).then_some(value)
}

/// One synchronous measurement of disk and memory usage.
#[derive(Debug, Default, Clone, Copy)]
struct ResourceSample {
    disk_percent: Option<u8>,
    memory_percent: Option<u8>,
}

impl ResourceSample {
    /// Blocking: performs filesystem syscalls and reads `/proc` / `/sys`.
    fn collect(data_dir: &Path) -> Self {
        Self {
            disk_percent: disk_usage_percent(data_dir),
            memory_percent: memory_usage_percent(),
        }
    }
}

/// `df`-style usage percentage: `used / (used + available)`, rounded up.
///
/// Blocks reserved for root are excluded from the denominator, matching the
/// `Use%` column of `df`.
fn disk_usage_from_blocks(total: u64, free: u64, available: u64) -> Option<u8> {
    let used = total.checked_sub(free)?;
    percent_ceil(used, used.checked_add(available)?)
}

/// `numerator / denominator` as a percentage rounded up, capped at 100.
fn percent_ceil(numerator: u64, denominator: u64) -> Option<u8> {
    if denominator == 0 {
        return None;
    }
    let pct = (u128::from(numerator) * 100).div_ceil(u128::from(denominator));
    Some(pct.min(100) as u8)
}

/// Walk up from `path` to the closest ancestor that exists, so a data
/// directory that has not been created yet still maps to its filesystem.
fn existing_ancestor(path: &Path) -> Option<&Path> {
    path.ancestors()
        .find(|p| !p.as_os_str().is_empty() && p.exists())
        .or_else(|| Some(Path::new(".")).filter(|_| path.is_relative()))
}

#[cfg(unix)]
fn disk_usage_percent(path: &Path) -> Option<u8> {
    fn to_u64<T: TryInto<u64>>(value: T) -> Option<u64> {
        value.try_into().ok()
    }
    let target = existing_ancestor(path)?;
    let stat = rustix::fs::statvfs(target).ok()?;
    disk_usage_from_blocks(
        to_u64(stat.f_blocks)?,
        to_u64(stat.f_bfree)?,
        to_u64(stat.f_bavail)?,
    )
}

#[cfg(not(unix))]
fn disk_usage_percent(_path: &Path) -> Option<u8> {
    None
}

/// Resident set size of this process as a percentage of the effective memory
/// limit (cgroup limit if set, otherwise total physical memory).
fn memory_usage_percent() -> Option<u8> {
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| parse_vm_rss_bytes(&s))?;
    let limit = effective_memory_limit_bytes()?;
    percent_ceil(rss, limit)
}

fn effective_memory_limit_bytes() -> Option<u64> {
    let mem_total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_meminfo_total_bytes(&s));
    let cgroup = cgroup_memory_limit_bytes();
    match (cgroup, mem_total) {
        (Some(c), Some(t)) => Some(c.min(t)),
        (c, t) => c.or(t),
    }
}

fn cgroup_memory_limit_bytes() -> Option<u64> {
    const CGROUP_ROOT: &str = "/sys/fs/cgroup";

    let mut candidates: Vec<PathBuf> = Vec::with_capacity(3);
    // cgroup v2: the process's own cgroup (works without a cgroup namespace),
    // then the namespace root (typical inside containers).
    if let Some(rel) = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|s| parse_cgroup_v2_path(&s))
    {
        let rel = rel.trim_start_matches('/');
        if !rel.is_empty() {
            candidates.push(Path::new(CGROUP_ROOT).join(rel).join("memory.max"));
        }
    }
    candidates.push(Path::new(CGROUP_ROOT).join("memory.max"));
    for path in &candidates {
        if let Ok(contents) = std::fs::read_to_string(path) {
            return parse_cgroup_limit(&contents);
        }
    }

    // cgroup v1.
    std::fs::read_to_string(Path::new(CGROUP_ROOT).join("memory/memory.limit_in_bytes"))
        .ok()
        .and_then(|s| parse_cgroup_limit(&s))
}

/// Parse `VmRSS:   12345 kB` from `/proc/self/status`; returns bytes.
fn parse_vm_rss_bytes(status: &str) -> Option<u64> {
    parse_kb_field(status, "VmRSS:")
}

/// Parse `MemTotal:   12345 kB` from `/proc/meminfo`; returns bytes.
fn parse_meminfo_total_bytes(meminfo: &str) -> Option<u64> {
    parse_kb_field(meminfo, "MemTotal:")
}

fn parse_kb_field(contents: &str, key: &str) -> Option<u64> {
    let line = contents.lines().find(|l| l.starts_with(key))?;
    let mut parts = line[key.len()..].split_whitespace();
    let value: u64 = parts.next()?.parse().ok()?;
    match parts.next() {
        Some(unit) if unit.eq_ignore_ascii_case("kb") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

/// Parse a cgroup memory limit file (`memory.max` or `memory.limit_in_bytes`).
/// Returns `None` when the limit is unset (`max`) or effectively unlimited.
fn parse_cgroup_limit(contents: &str) -> Option<u64> {
    let value = contents.trim();
    if value == "max" {
        return None;
    }
    let limit: u64 = value.parse().ok()?;
    (limit > 0 && limit < CGROUP_UNLIMITED_THRESHOLD).then_some(limit)
}

/// Extract the unified (v2) hierarchy path from `/proc/self/cgroup`
/// (the `0::<path>` line).
fn parse_cgroup_v2_path(contents: &str) -> Option<String> {
    contents
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| p.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closed_when_thresholds_not_exceeded() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 10);
        breaker.set_queue_depth(10);
        assert!(breaker.check().await.is_ok());
    }

    #[tokio::test]
    async fn open_when_queue_depth_exceeds_threshold() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 1);
        breaker.set_queue_depth(2);
        assert!(breaker.check().await.is_err());
    }

    #[tokio::test]
    async fn tripped_breaker_is_retryable_unavailable() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 1);
        breaker.set_queue_depth(2);
        let status = breaker.check_status().await.expect_err("breaker trips");
        // RESOURCE_EXHAUSTED without RetryInfo is non-retryable for OTLP
        // exporters; a transient overload must be UNAVAILABLE.
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            status.message().contains("queue depth"),
            "{}",
            status.message()
        );
        assert_eq!(
            crate::retry::retry_delay(&status),
            Some(RESOURCE_SAMPLE_INTERVAL)
        );
    }

    #[tokio::test]
    async fn exposes_queue_depth_snapshot() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 10);
        breaker.set_queue_depth(7);
        assert_eq!(breaker.queue_depth(), 7);
        assert_eq!(breaker.max_queue_depth(), 10);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn measures_disk_and_memory_in_process() {
        let dir = std::env::temp_dir().join("zradar-cb-not-created-yet");
        let breaker = CircuitBreaker::new(dir, 100, 100, 10);
        breaker
            .check()
            .await
            .expect("thresholds of 100% never trip");
        let disk = breaker
            .disk_usage_percent()
            .expect("statvfs works on linux");
        assert!(disk <= 100);
        let mem = breaker
            .memory_usage_percent()
            .expect("/proc is readable on linux");
        assert!(mem <= 100);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_when_disk_threshold_is_zero_and_disk_in_use() {
        let breaker = CircuitBreaker::new(std::env::temp_dir(), 0, 100, 10);
        // Any mounted filesystem holding a temp dir has some blocks in use.
        let err = breaker.check().await.expect_err("0% disk threshold trips");
        assert!(err.contains("disk usage"), "{err}");
    }

    #[test]
    fn disk_usage_matches_df_semantics() {
        // df: 1000 blocks, 400 free, 350 available to non-root -> used 600,
        // Use% = ceil(600 / (600 + 350)) = 64%.
        assert_eq!(disk_usage_from_blocks(1000, 400, 350), Some(64));
        assert_eq!(disk_usage_from_blocks(1000, 1000, 1000), Some(0));
        assert_eq!(disk_usage_from_blocks(1000, 0, 0), Some(100));
        assert_eq!(disk_usage_from_blocks(0, 0, 0), None);
        assert_eq!(disk_usage_from_blocks(10, 20, 20), None);
    }

    #[test]
    fn percent_rounds_up_and_caps() {
        assert_eq!(percent_ceil(1, 3), Some(34));
        assert_eq!(percent_ceil(5, 4), Some(100));
        assert_eq!(percent_ceil(1, 0), None);
        assert_eq!(percent_ceil(u64::MAX, u64::MAX), Some(100));
    }

    #[test]
    fn parses_vm_rss() {
        let status = "Name:\tzradar\nVmPeak:\t  900000 kB\nVmRSS:\t  123456 kB\nThreads:\t8\n";
        assert_eq!(parse_vm_rss_bytes(status), Some(123_456 * 1024));
        assert_eq!(parse_vm_rss_bytes("Name:\tzradar\n"), None);
        assert_eq!(parse_vm_rss_bytes("VmRSS:\tgarbage kB\n"), None);
    }

    #[test]
    fn parses_meminfo_total() {
        let meminfo = "MemTotal:       16318412 kB\nMemFree:         1234567 kB\n";
        assert_eq!(parse_meminfo_total_bytes(meminfo), Some(16_318_412 * 1024));
        assert_eq!(parse_meminfo_total_bytes("MemFree: 1 kB\n"), None);
    }

    #[test]
    fn parses_cgroup_limits() {
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("536870912\n"), Some(536_870_912));
        // cgroup v1 "unlimited"
        assert_eq!(parse_cgroup_limit("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_limit("0\n"), None);
        assert_eq!(parse_cgroup_limit(""), None);
    }

    #[test]
    fn parses_cgroup_v2_path() {
        let v2 = "0::/kubepods.slice/pod123/abc\n";
        assert_eq!(
            parse_cgroup_v2_path(v2).as_deref(),
            Some("/kubepods.slice/pod123/abc")
        );
        let hybrid = "12:memory:/user.slice\n0::/user.slice/session-1.scope\n";
        assert_eq!(
            parse_cgroup_v2_path(hybrid).as_deref(),
            Some("/user.slice/session-1.scope")
        );
        assert_eq!(parse_cgroup_v2_path("12:memory:/docker/abc\n"), None);
    }

    #[test]
    fn existing_ancestor_walks_up() {
        let tmp = std::env::temp_dir();
        let missing = tmp.join("zradar-cb-missing").join("deeper");
        assert_eq!(existing_ancestor(&missing), Some(tmp.as_path()));
        assert_eq!(
            existing_ancestor(Path::new("does-not-exist-xyz")),
            Some(Path::new("."))
        );
    }
}
