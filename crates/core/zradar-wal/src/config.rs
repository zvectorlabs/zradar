/// Configuration for the disk-resident WAL.
///
/// All knobs have sensible defaults sized for a 1 vCPU / 4 GiB RAM deployment.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct WalConfig {
    /// Enable the WAL. When false, OTLP services bypass the WAL entirely.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Directory for WAL segment files and checkpoint.
    #[serde(default = "default_wal_dir")]
    pub wal_dir: String,

    /// Maximum bytes per segment before sealing and opening a new one.
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: u64,

    /// How often the flusher drains WAL records to Parquet (ms).
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,

    /// Window for group-commit fsync coalescing (ms).
    #[serde(default = "default_group_commit_window_ms")]
    pub group_commit_window_ms: u64,

    /// WAL fill percentage at which slowdown begins.
    #[serde(default = "default_backpressure_warn_pct")]
    pub backpressure_warn_pct: u8,

    /// WAL fill percentage at which writes are rejected.
    #[serde(default = "default_backpressure_reject_pct")]
    pub backpressure_reject_pct: u8,

    /// Maximum delay (ms) injected during slowdown stage.
    #[serde(default = "default_slowdown_max_delay_ms")]
    pub slowdown_max_delay_ms: u64,

    /// Minimum free bytes on wal_dir filesystem before forced rejection.
    #[serde(default = "default_wal_min_free_bytes")]
    pub wal_min_free_bytes: u64,

    /// Records between disk-space checks via statvfs.
    #[serde(default = "default_disk_check_interval")]
    pub disk_check_interval: u64,

    /// Maximum bytes replayed per batch during startup replay.
    #[serde(default = "default_replay_batch_max_bytes")]
    pub replay_batch_max_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            wal_dir: default_wal_dir(),
            segment_max_bytes: default_segment_max_bytes(),
            flush_interval_ms: default_flush_interval_ms(),
            group_commit_window_ms: default_group_commit_window_ms(),
            backpressure_warn_pct: default_backpressure_warn_pct(),
            backpressure_reject_pct: default_backpressure_reject_pct(),
            slowdown_max_delay_ms: default_slowdown_max_delay_ms(),
            wal_min_free_bytes: default_wal_min_free_bytes(),
            disk_check_interval: default_disk_check_interval(),
            replay_batch_max_bytes: default_replay_batch_max_bytes(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
fn default_wal_dir() -> String {
    "./data/wal".to_string()
}
fn default_segment_max_bytes() -> u64 {
    256 * 1024 * 1024 // 256 MiB
}
fn default_flush_interval_ms() -> u64 {
    200
}
fn default_group_commit_window_ms() -> u64 {
    1
}
fn default_backpressure_warn_pct() -> u8 {
    70
}
fn default_backpressure_reject_pct() -> u8 {
    95
}
fn default_slowdown_max_delay_ms() -> u64 {
    50
}
fn default_wal_min_free_bytes() -> u64 {
    1024 * 1024 * 1024 // 1 GiB
}
fn default_disk_check_interval() -> u64 {
    1024
}
fn default_replay_batch_max_bytes() -> u64 {
    64 * 1024 * 1024 // 64 MiB
}
