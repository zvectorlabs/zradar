//! Free-space guard for the WAL filesystem.
//!
//! The WAL must never fill the disk: a full filesystem turns every append into
//! an I/O error and can corrupt the tail of the active segment. The
//! [`DiskSpaceGuard`] samples the free space of the WAL directory with
//! `statvfs(2)` and refuses new appends with a retryable
//! [`AppendError::InsufficientDiskSpace`] once it drops below
//! `wal_min_free_bytes`.
//!
//! `statvfs` is a blocking syscall, so samples are taken on the blocking
//! thread pool and cached. A sample is refreshed every `disk_check_interval`
//! append attempts or after [`MAX_SAMPLE_AGE`], whichever comes first. The
//! time bound matters while writes are refused: rejected attempts still count,
//! and the age bound guarantees recovery is noticed even under light traffic.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::group_commit::AppendError;

/// Maximum age of a free-space sample before it is refreshed.
pub const MAX_SAMPLE_AGE: Duration = Duration::from_secs(1);

/// Sentinel stored when free space has never been sampled.
const NEVER_SAMPLED: u64 = u64::MAX;
/// Sentinel stored when free space could not be measured.
const UNKNOWN_FREE: u64 = u64::MAX;

/// Bytes available to unprivileged writers on the filesystem holding `path`
/// (`f_bavail * f_frsize`, the `Avail` column of `df`).
///
/// Blocking: call it from [`tokio::task::spawn_blocking`] on async paths.
#[cfg(unix)]
pub fn available_bytes(path: &Path) -> std::io::Result<u64> {
    // Field widths differ across Unix platforms; widen generically.
    fn to_u64<T: TryInto<u64>>(value: T) -> u64 {
        value.try_into().unwrap_or(u64::MAX)
    }
    let stat = rustix::fs::statvfs(path)?;
    Ok(to_u64(stat.f_bavail).saturating_mul(to_u64(stat.f_frsize)))
}

/// Free-space measurement is only implemented on Unix.
#[cfg(not(unix))]
pub fn available_bytes(_path: &Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "free-space check is not supported on this platform",
    ))
}

/// Refuses WAL appends while the WAL filesystem is low on free space.
#[derive(Debug)]
pub struct DiskSpaceGuard {
    dir: PathBuf,
    min_free_bytes: u64,
    check_every: u64,
    attempts: AtomicU64,
    /// Last sampled free bytes, or [`UNKNOWN_FREE`].
    free_bytes: AtomicU64,
    created_at: Instant,
    /// Milliseconds since `created_at` of the last sample, or [`NEVER_SAMPLED`].
    last_sample_ms: AtomicU64,
    sampling: AtomicBool,
    warned_unavailable: AtomicBool,
}

impl DiskSpaceGuard {
    /// `min_free_bytes == 0` disables the guard. `check_every == 0` is
    /// treated as "sample on every append".
    pub fn new(dir: impl Into<PathBuf>, min_free_bytes: u64, check_every: u64) -> Self {
        Self {
            dir: dir.into(),
            min_free_bytes,
            check_every: check_every.max(1),
            attempts: AtomicU64::new(0),
            free_bytes: AtomicU64::new(UNKNOWN_FREE),
            created_at: Instant::now(),
            last_sample_ms: AtomicU64::new(NEVER_SAMPLED),
            sampling: AtomicBool::new(false),
            warned_unavailable: AtomicBool::new(false),
        }
    }

    /// Whether the guard is active.
    pub fn enabled(&self) -> bool {
        self.min_free_bytes > 0
    }

    /// Last sampled free bytes on the WAL filesystem (`None` if unknown).
    pub fn free_bytes(&self) -> Option<u64> {
        let v = self.free_bytes.load(Ordering::Relaxed);
        (v != UNKNOWN_FREE).then_some(v)
    }

    /// Admit or refuse one append. Refreshes the cached sample when it is due.
    ///
    /// Fails open (admits) when free space cannot be measured; a warning is
    /// logged once in that case.
    pub async fn admit(&self) -> Result<(), AppendError> {
        if !self.enabled() {
            return Ok(());
        }
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        if self.sample_due(attempt) {
            self.refresh().await;
        }
        match self.free_bytes() {
            Some(available) if available < self.min_free_bytes => {
                Err(AppendError::InsufficientDiskSpace {
                    available,
                    required: self.min_free_bytes,
                })
            }
            _ => Ok(()),
        }
    }

    /// Take a fresh sample now (on the blocking pool).
    pub async fn refresh(&self) {
        if self
            .sampling
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another caller is sampling; keep using the cached value.
            return;
        }

        let dir = self.dir.clone();
        let sample = tokio::task::spawn_blocking(move || available_bytes(&dir))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));

        match sample {
            Ok(available) => {
                // Never collide with the "unknown" sentinel.
                self.free_bytes
                    .store(available.min(UNKNOWN_FREE - 1), Ordering::Relaxed);
            }
            Err(error) => {
                self.free_bytes.store(UNKNOWN_FREE, Ordering::Relaxed);
                if !self.warned_unavailable.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        wal_dir = %self.dir.display(),
                        %error,
                        "WAL free-space check unavailable; appends are not guarded"
                    );
                }
            }
        }
        self.last_sample_ms
            .store(self.elapsed_ms(), Ordering::Release);
        self.sampling.store(false, Ordering::Release);
    }

    fn sample_due(&self, attempt: u64) -> bool {
        let last = self.last_sample_ms.load(Ordering::Acquire);
        if last == NEVER_SAMPLED || attempt.is_multiple_of(self.check_every) {
            return true;
        }
        self.elapsed_ms().saturating_sub(last) >= MAX_SAMPLE_AGE.as_millis() as u64
    }

    fn elapsed_ms(&self) -> u64 {
        (self.created_at.elapsed().as_millis() as u64).min(NEVER_SAMPLED - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    fn measures_free_space_of_existing_dir() {
        let tmp = TempDir::new().unwrap();
        let free = available_bytes(tmp.path()).expect("statvfs works on unix");
        assert!(free > 0, "a writable temp dir has some free space");
    }

    #[test]
    fn missing_dir_is_an_error() {
        let tmp = TempDir::new().unwrap();
        assert!(available_bytes(&tmp.path().join("missing")).is_err());
    }

    #[tokio::test]
    async fn disabled_guard_always_admits() {
        let tmp = TempDir::new().unwrap();
        let guard = DiskSpaceGuard::new(tmp.path(), 0, 1);
        assert!(!guard.enabled());
        guard.admit().await.unwrap();
        // Never sampled because the guard is off.
        assert_eq!(guard.free_bytes(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_appends_below_threshold() {
        let tmp = TempDir::new().unwrap();
        // No filesystem has u64::MAX - 1 bytes free.
        let guard = DiskSpaceGuard::new(tmp.path(), u64::MAX - 1, 1024);
        match guard.admit().await {
            Err(AppendError::InsufficientDiskSpace {
                available,
                required,
            }) => {
                assert_eq!(required, u64::MAX - 1);
                assert!(available < required);
                assert_eq!(guard.free_bytes(), Some(available));
            }
            other => panic!("expected InsufficientDiskSpace, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn admits_appends_above_threshold() {
        let tmp = TempDir::new().unwrap();
        let guard = DiskSpaceGuard::new(tmp.path(), 1, 1024);
        guard.admit().await.unwrap();
        assert!(guard.free_bytes().is_some_and(|b| b >= 1));
    }

    #[tokio::test]
    async fn fails_open_when_free_space_is_unknown() {
        let tmp = TempDir::new().unwrap();
        let guard = DiskSpaceGuard::new(tmp.path().join("missing"), u64::MAX - 1, 1);
        guard.admit().await.unwrap();
        assert_eq!(guard.free_bytes(), None);
    }

    #[tokio::test]
    async fn samples_every_n_attempts_or_when_stale() {
        let tmp = TempDir::new().unwrap();
        let guard = DiskSpaceGuard::new(tmp.path(), 1, 4);
        assert!(guard.sample_due(1), "first attempt always samples");
        guard.refresh().await;
        assert!(!guard.sample_due(1));
        assert!(!guard.sample_due(3));
        assert!(guard.sample_due(4));
        assert!(guard.sample_due(8));

        // A stale sample is refreshed regardless of the attempt count.
        tokio::time::sleep(MAX_SAMPLE_AGE + Duration::from_millis(50)).await;
        assert!(guard.sample_due(1));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recovers_once_space_is_available_again() {
        let tmp = TempDir::new().unwrap();
        let guard = DiskSpaceGuard::new(tmp.path(), 1, 1_000_000);
        // Simulate a previous low-space sample.
        guard.free_bytes.store(0, Ordering::Relaxed);
        guard.last_sample_ms.store(0, Ordering::Release);
        tokio::time::sleep(MAX_SAMPLE_AGE + Duration::from_millis(50)).await;
        // The stale sample is refreshed even though few attempts were made.
        guard.admit().await.unwrap();
        assert!(guard.free_bytes().is_some_and(|b| b > 0));
    }
}
