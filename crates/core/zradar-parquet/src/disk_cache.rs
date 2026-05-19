//! DiskCache — local disk cache for Parquet files stored in S3.
//!
//! When the query path encounters a file whose `location = "s3"`, it calls
//! `DiskCache::get_or_fetch` which either:
//! - Returns the local cached path if the file is still valid, or
//! - Downloads from S3, writes to the cache directory, and returns the new path.
//!
//! Eviction is LRU + TTL:
//! - Files older than `cache_ttl_secs` are evicted proactively.
//! - When the cache exceeds `cache_max_size_bytes`, the least-recently-used
//!   files are evicted until the cache is within budget.
//!
//! All S3 access goes through `Arc<dyn BlockStorage>` so the backend is
//! swappable without touching this code.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::sync::RwLock;
use tracing::{debug, info};
use zradar_models::ParquetStorageConfig;
use zradar_traits::BlockStorage;

/// Metadata tracked for each file held in the disk cache.
#[derive(Debug)]
struct CacheEntry {
    /// Absolute path on local disk.
    local_path: PathBuf,
    /// File size in bytes.
    size_bytes: u64,
    /// Wall-clock time when this entry was last accessed.
    last_accessed: Instant,
    /// Wall-clock time when this entry was first created.
    created_at: Instant,
}

/// Local disk cache for S3-backed Parquet files.
pub struct DiskCache {
    /// In-memory index: S3 key → CacheEntry.
    index: RwLock<HashMap<String, CacheEntry>>,
    /// Block storage used to download missing files.
    block_storage: Arc<dyn BlockStorage>,
    /// Root directory where cached files are written.
    cache_dir: PathBuf,
    /// Maximum total bytes allowed in the cache.
    max_size_bytes: u64,
    /// Time-to-live for a cache entry.
    ttl: Duration,
}

impl DiskCache {
    /// Create a new `DiskCache`.
    pub fn new(block_storage: Arc<dyn BlockStorage>, config: &ParquetStorageConfig) -> Self {
        Self {
            index: RwLock::new(HashMap::new()),
            block_storage,
            cache_dir: PathBuf::from(&config.cache_dir),
            max_size_bytes: config.cache_max_size_bytes,
            ttl: Duration::from_secs(config.cache_ttl_secs),
        }
    }

    /// Return a local file path for `s3_key`, downloading from S3 if necessary.
    ///
    /// The returned `PathBuf` always points to a readable local file.
    pub async fn get_or_fetch(&self, s3_key: &str) -> anyhow::Result<PathBuf> {
        // Fast path: check the index under a read lock.
        {
            let index = self.index.read().await;
            if let Some(entry) = index.get(s3_key)
                && entry.created_at.elapsed() < self.ttl
                && entry.local_path.exists()
            {
                debug!(s3_key, "DiskCache: cache hit");
                // We'll update last_accessed under a write lock below.
                let path = entry.local_path.clone();
                drop(index);
                self.touch(s3_key).await;
                return Ok(path);
            }
        }

        // Slow path: download the file and insert into cache.
        self.fetch_and_insert(s3_key).await
    }

    /// Update `last_accessed` for an existing entry.
    async fn touch(&self, s3_key: &str) {
        let mut index = self.index.write().await;
        if let Some(entry) = index.get_mut(s3_key) {
            entry.last_accessed = Instant::now();
        }
    }

    /// Download `s3_key` from S3, write to `cache_dir`, insert into index.
    async fn fetch_and_insert(&self, s3_key: &str) -> anyhow::Result<PathBuf> {
        tokio::fs::create_dir_all(&self.cache_dir)
            .await
            .context("Failed to create cache directory")?;

        let data = self
            .block_storage
            .download(s3_key)
            .await
            .with_context(|| format!("Failed to download S3 key: {s3_key}"))?;

        // Derive a safe local filename from the S3 key.
        let file_name = s3_key.replace(['/', ':'], "_");
        let local_path = self.cache_dir.join(&file_name);

        tokio::fs::write(&local_path, &data)
            .await
            .context("Failed to write cached file")?;

        let size_bytes = data.len() as u64;
        let now = Instant::now();

        {
            let mut index = self.index.write().await;
            index.insert(
                s3_key.to_string(),
                CacheEntry {
                    local_path: local_path.clone(),
                    size_bytes,
                    last_accessed: now,
                    created_at: now,
                },
            );
        }

        info!(s3_key, size_bytes, "DiskCache: fetched and cached file");

        // Evict if over budget.
        self.evict_if_needed().await;

        Ok(local_path)
    }

    /// Evict expired entries first, then LRU entries until within budget.
    async fn evict_if_needed(&self) {
        let mut index = self.index.write().await;

        // Remove TTL-expired entries.
        let expired: Vec<String> = index
            .iter()
            .filter(|(_, e)| e.created_at.elapsed() >= self.ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired {
            if let Some(entry) = index.remove(&key) {
                let _ = std::fs::remove_file(&entry.local_path);
                debug!(key, "DiskCache: evicted expired entry");
            }
        }

        // Evict LRU entries if total size exceeds budget.
        let total: u64 = index.values().map(|e| e.size_bytes).sum();
        if total <= self.max_size_bytes {
            return;
        }

        let mut entries: Vec<(String, Instant, u64)> = index
            .iter()
            .map(|(k, e)| (k.clone(), e.last_accessed, e.size_bytes))
            .collect();

        // Sort by last_accessed ascending (oldest first).
        entries.sort_by_key(|(_, ts, _)| *ts);

        let mut running_total: u64 = index.values().map(|e| e.size_bytes).sum();

        for (key, _, size) in entries {
            if running_total <= self.max_size_bytes {
                break;
            }
            if let Some(entry) = index.remove(&key) {
                let _ = std::fs::remove_file(&entry.local_path);
                running_total = running_total.saturating_sub(size);
                debug!(key, "DiskCache: evicted LRU entry");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeStorage {
        data: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl BlockStorage for FakeStorage {
        async fn upload(&self, _key: &str, _data: &[u8]) -> anyhow::Result<String> {
            Ok("s3://test/key".to_string())
        }
        async fn download(&self, _key: &str) -> anyhow::Result<Vec<u8>> {
            Ok(self.data.clone())
        }
        async fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn cleanup(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_get_or_fetch_downloads_and_caches() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = ParquetStorageConfig {
            cache_dir: dir.path().to_string_lossy().into_owned(),
            cache_max_size_bytes: 10_000,
            cache_ttl_secs: 3600,
            ..ParquetStorageConfig::default()
        };

        let storage = Arc::new(FakeStorage {
            data: b"hello parquet".to_vec(),
        });
        let cache = DiskCache::new(storage, &config);

        let path = cache
            .get_or_fetch("tenant/2024/file.parquet")
            .await
            .unwrap();
        assert!(path.exists());
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"hello parquet");
    }

    #[tokio::test]
    async fn test_get_or_fetch_cache_hit_on_second_call() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = ParquetStorageConfig {
            cache_dir: dir.path().to_string_lossy().into_owned(),
            cache_max_size_bytes: 10_000,
            cache_ttl_secs: 3600,
            ..ParquetStorageConfig::default()
        };

        let storage = Arc::new(FakeStorage {
            data: b"data".to_vec(),
        });
        let cache = DiskCache::new(storage, &config);

        let path1 = cache.get_or_fetch("some/key.parquet").await.unwrap();
        let path2 = cache.get_or_fetch("some/key.parquet").await.unwrap();
        // Both calls return the same path
        assert_eq!(path1, path2);
    }
}
