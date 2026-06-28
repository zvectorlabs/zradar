//! Ingestor configuration

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct IngestorConfig {
    #[serde(default)]
    pub storage: StorageConfig,

    /// Disk-resident Write-Ahead Log configuration (Phase 08).
    #[serde(default)]
    pub wal: WalIngestorConfig,
}

/// WAL configuration embedded in the ingestor TOML section.
///
/// Mirrors `zradar_wal::config::WalConfig` for TOML deserialization.
/// The server converts this to the WAL crate's native config type.
#[derive(Debug, Clone, Deserialize)]
pub struct WalIngestorConfig {
    #[serde(default = "default_wal_dir")]
    pub wal_dir: String,

    #[serde(default = "default_wal_segment_max_bytes")]
    pub segment_max_bytes: u64,

    #[serde(default = "default_wal_flush_interval_ms")]
    pub flush_interval_ms: u64,

    #[serde(default = "default_wal_group_commit_window_ms")]
    pub group_commit_window_ms: u64,

    #[serde(default = "default_wal_replay_batch_max_bytes")]
    pub replay_batch_max_bytes: u64,
}

impl Default for WalIngestorConfig {
    fn default() -> Self {
        Self {
            wal_dir: default_wal_dir(),
            segment_max_bytes: default_wal_segment_max_bytes(),
            flush_interval_ms: default_wal_flush_interval_ms(),
            group_commit_window_ms: default_wal_group_commit_window_ms(),
            replay_batch_max_bytes: default_wal_replay_batch_max_bytes(),
        }
    }
}

fn default_wal_dir() -> String {
    "./data/wal".to_string()
}
fn default_wal_segment_max_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_wal_flush_interval_ms() -> u64 {
    200
}
fn default_wal_group_commit_window_ms() -> u64 {
    1
}
fn default_wal_replay_batch_max_bytes() -> u64 {
    64 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3: Option<S3StorageConfig>,

    /// Root directory for Parquet files written by the direct-write path.
    ///
    /// Files are written under `{parquet_data_dir}/files/{tenant}/...`
    /// Default: `"./data/parquet-files"`
    #[serde(default = "default_parquet_data_dir")]
    pub parquet_data_dir: String,

    /// Configuration for Parquet file lifecycle management (FileMover, DiskCache, Retention).
    /// Only used when `storage_type = "s3"`.
    #[serde(default)]
    pub parquet: ParquetStorageConfig,
}

/// Parquet file lifecycle configuration.
///
/// Controls how local Parquet files are moved to S3, cached locally when
/// re-read, and eventually deleted by the retention job.
#[derive(Debug, Clone, Deserialize)]
pub struct ParquetStorageConfig {
    /// Seconds to wait after a file is written before moving it to S3.
    /// Prevents uploading files that are still being written.
    /// Default: 60
    #[serde(default = "default_file_push_delay_secs")]
    pub file_push_delay_secs: u64,

    /// How often (seconds) the FileMover job wakes up to scan for files to push.
    /// Default: 30
    #[serde(default = "default_file_push_interval_secs")]
    pub file_push_interval_secs: u64,

    /// Seconds after a file has been moved to S3 before deleting the local copy.
    /// Default: 300
    #[serde(default = "default_file_delete_local_delay_secs")]
    pub file_delete_local_delay_secs: u64,

    /// Directory for the local disk cache of S3 files.
    /// Default: `"./data/parquet-cache"`
    #[serde(default = "default_cache_dir")]
    pub cache_dir: String,

    /// Maximum total bytes allowed in the disk cache before LRU eviction.
    /// Default: 1 GiB
    #[serde(default = "default_cache_max_size_bytes")]
    pub cache_max_size_bytes: u64,

    /// Time-to-live in seconds for a cached file before it is evicted.
    /// Default: 3600 (1 hour)
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,

    /// How often (seconds) the retention job scans for expired files.
    /// Default: 3600
    #[serde(default = "default_retention_check_interval_secs")]
    pub retention_check_interval_secs: u64,

    /// How often (seconds) the storage-usage snapshot job runs.
    /// Since it snapshots the previous day's immutable data, once per day is
    /// sufficient. Default: 86400
    #[serde(default = "default_storage_snapshot_interval_secs")]
    pub storage_snapshot_interval_secs: u64,

    /// Default retention in days for files not covered by a project override.
    /// Default: 30
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,

    #[serde(default = "default_circuit_breaker_max_disk_usage_percent")]
    pub circuit_breaker_max_disk_usage_percent: u8,

    #[serde(default = "default_circuit_breaker_max_memory_usage_percent")]
    pub circuit_breaker_max_memory_usage_percent: u8,

    #[serde(default = "default_circuit_breaker_max_queue_depth")]
    pub circuit_breaker_max_queue_depth: u64,

    // -----------------------------------------------------------------------
    // M07-02: Bloom filters
    // -----------------------------------------------------------------------
    /// Parquet columns to build bloom filters on at write time.
    /// Enables row-group skipping for point lookups (trace_id, span_id, id).
    /// Default: ["trace_id", "span_id", "id"]
    #[serde(default = "default_bloom_filter_columns")]
    pub bloom_filter_columns: Vec<String>,

    // -----------------------------------------------------------------------
    // M07-03: Atomic write safety
    // -----------------------------------------------------------------------
    /// When true, fsync the temp .par file before the atomic rename to .parquet.
    /// Set to false in dev/test to skip the disk flush for speed.
    /// Default: true
    #[serde(default = "default_fsync_before_rename")]
    pub fsync_before_rename: bool,

    // -----------------------------------------------------------------------
    // M07-04: Write buffer
    // -----------------------------------------------------------------------
    /// Enable in-memory write buffering. When true, insert_* calls accumulate
    /// data in the WriteBuffer and a FlushWorker handles batched Parquet writes.
    /// Default: true
    #[serde(default = "default_write_buffer_enabled")]
    pub write_buffer_enabled: bool,

    /// Maximum bytes per (tenant, project, signal, hour) slot before a flush
    /// is triggered regardless of the TTL.
    /// Default: 8 MiB
    #[serde(default = "default_write_buffer_size_bytes")]
    pub write_buffer_size_bytes: usize,

    /// How often (seconds) the FlushWorker drains TTL-expired buffer slots.
    /// Default: 30
    #[serde(default = "default_write_buffer_flush_interval_secs")]
    pub write_buffer_flush_interval_secs: u64,

    // -----------------------------------------------------------------------
    // M07-06: In-memory Parquet cache
    // -----------------------------------------------------------------------
    /// Enable in-memory caching of hot Parquet file bytes (M07-06).
    /// Sits above DiskCache; eliminates disk I/O for frequently read S3 files.
    /// Default: true
    #[serde(default = "default_memory_cache_enabled")]
    pub memory_cache_enabled: bool,

    /// Maximum total bytes allowed in the memory cache.
    /// Default: 256 MiB
    #[serde(default = "default_memory_cache_max_bytes")]
    pub memory_cache_max_bytes: u64,

    /// Number of shards in the memory cache (reduces lock contention).
    /// Default: 16
    #[serde(default = "default_memory_cache_shards")]
    pub memory_cache_shards: usize,

    // -----------------------------------------------------------------------
    // M07-07: Compaction
    // -----------------------------------------------------------------------
    /// Enable the background compaction job (M07-07).
    /// Merges small Parquet files within the same (tenant, project, signal, date)
    /// bucket into a single larger file, reducing per-query file count.
    /// Default: true
    #[serde(default = "default_compaction_enabled")]
    pub compaction_enabled: bool,

    /// How often (seconds) the compaction job scans for merge candidates.
    /// Default: 3600
    #[serde(default = "default_compaction_check_interval_secs")]
    pub compaction_check_interval_secs: u64,

    /// Minimum number of files in a bucket before compaction is triggered.
    /// Default: 4
    #[serde(default = "default_compaction_min_files")]
    pub compaction_min_files: usize,

    /// Only compact individual files smaller than this byte threshold.
    /// Avoids re-compacting already large files.
    /// Default: 52_428_800 (50 MiB)
    #[serde(default = "default_compaction_max_file_size_bytes")]
    pub compaction_max_file_size_bytes: i64,
}

impl Default for ParquetStorageConfig {
    fn default() -> Self {
        Self {
            file_push_delay_secs: default_file_push_delay_secs(),
            file_push_interval_secs: default_file_push_interval_secs(),
            file_delete_local_delay_secs: default_file_delete_local_delay_secs(),
            cache_dir: default_cache_dir(),
            cache_max_size_bytes: default_cache_max_size_bytes(),
            cache_ttl_secs: default_cache_ttl_secs(),
            retention_check_interval_secs: default_retention_check_interval_secs(),
            storage_snapshot_interval_secs: default_storage_snapshot_interval_secs(),
            retention_days: default_retention_days(),
            circuit_breaker_max_disk_usage_percent: default_circuit_breaker_max_disk_usage_percent(
            ),
            circuit_breaker_max_memory_usage_percent:
                default_circuit_breaker_max_memory_usage_percent(),
            circuit_breaker_max_queue_depth: default_circuit_breaker_max_queue_depth(),
            bloom_filter_columns: default_bloom_filter_columns(),
            fsync_before_rename: default_fsync_before_rename(),
            write_buffer_enabled: default_write_buffer_enabled(),
            write_buffer_size_bytes: default_write_buffer_size_bytes(),
            write_buffer_flush_interval_secs: default_write_buffer_flush_interval_secs(),
            memory_cache_enabled: default_memory_cache_enabled(),
            memory_cache_max_bytes: default_memory_cache_max_bytes(),
            memory_cache_shards: default_memory_cache_shards(),
            compaction_enabled: default_compaction_enabled(),
            compaction_check_interval_secs: default_compaction_check_interval_secs(),
            compaction_min_files: default_compaction_min_files(),
            compaction_max_file_size_bytes: default_compaction_max_file_size_bytes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
}

// Default functions
fn default_parquet_data_dir() -> String {
    "./data/parquet-files".to_string()
}
fn default_file_push_delay_secs() -> u64 {
    60
}
fn default_file_push_interval_secs() -> u64 {
    30
}
fn default_file_delete_local_delay_secs() -> u64 {
    300
}
fn default_cache_dir() -> String {
    "./data/parquet-cache".to_string()
}
fn default_cache_max_size_bytes() -> u64 {
    1_073_741_824 // 1 GiB
}
fn default_cache_ttl_secs() -> u64 {
    3600
}
fn default_retention_check_interval_secs() -> u64 {
    3600
}
fn default_storage_snapshot_interval_secs() -> u64 {
    86_400
}
fn default_retention_days() -> u32 {
    30
}
fn default_circuit_breaker_max_disk_usage_percent() -> u8 {
    95
}
fn default_circuit_breaker_max_memory_usage_percent() -> u8 {
    95
}
fn default_circuit_breaker_max_queue_depth() -> u64 {
    10_000
}
fn default_bloom_filter_columns() -> Vec<String> {
    vec!["trace_id".into(), "span_id".into(), "id".into()]
}
fn default_fsync_before_rename() -> bool {
    true
}
fn default_write_buffer_enabled() -> bool {
    true
}
fn default_write_buffer_size_bytes() -> usize {
    8 * 1024 * 1024 // 8 MiB
}
fn default_write_buffer_flush_interval_secs() -> u64 {
    30
}
fn default_memory_cache_enabled() -> bool {
    true
}
fn default_memory_cache_max_bytes() -> u64 {
    256 * 1024 * 1024 // 256 MiB
}
fn default_memory_cache_shards() -> usize {
    16
}
fn default_compaction_enabled() -> bool {
    true
}
fn default_compaction_check_interval_secs() -> u64 {
    3600
}
fn default_compaction_min_files() -> usize {
    4
}
fn default_compaction_max_file_size_bytes() -> i64 {
    52_428_800 // 50 MiB
}
