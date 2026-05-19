//! M07-06: Sharded in-memory LRU cache for hot Parquet file bytes.
//!
//! `MemoryCache` sits above `DiskCache` as a faster in-memory tier.  When the
//! query path fetches a file from S3 via `DiskCache`, the raw bytes are also
//! stored in `MemoryCache`.  On the next access, the bytes are served from
//! RAM — no disk I/O.
//!
//! ## Sharding
//!
//! A single global `Mutex<LruCache>` would serialize all cache operations.
//! Instead, `MemoryCache` maintains N independent shards, each protected by
//! its own `parking_lot::Mutex`.  Keys are routed to shards via
//! `FNV hash(key) % N` so hot keys spread across shards and lock contention
//! stays low under concurrent query load.
//!
//! ## Eviction
//!
//! Each shard is an `LruCache` bounded by byte count.  When inserting a new
//! entry would exceed the per-shard byte limit, the LRU entry is evicted
//! first.  The global byte limit is divided evenly across shards.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

/// Sharded in-memory LRU cache.
///
/// `K` must be `Clone + Hash + Eq` (file path strings work directly).
/// Values are stored as `Arc<Bytes>` so callers can hold references without
/// copying the bytes again.
pub struct MemoryCache {
    shards: Vec<Mutex<Shard>>,
    shard_count: usize,
}

struct Shard {
    cache: LruCache<String, Arc<Bytes>>,
    /// Current total bytes held by this shard.
    used_bytes: u64,
    /// Maximum bytes this shard is allowed to hold.
    max_bytes: u64,
}

impl Shard {
    fn new(max_bytes: u64) -> Self {
        Self {
            // Start unbounded; size is enforced via `used_bytes` + manual eviction.
            cache: LruCache::unbounded(),
            used_bytes: 0,
            max_bytes,
        }
    }

    fn insert(&mut self, key: String, value: Arc<Bytes>) {
        let new_size = value.len() as u64;

        // Evict LRU entries until we have room.
        while self.used_bytes + new_size > self.max_bytes {
            if let Some((_, evicted)) = self.cache.pop_lru() {
                self.used_bytes = self.used_bytes.saturating_sub(evicted.len() as u64);
            } else {
                break; // Cache is empty; store anyway (oversized single entry).
            }
        }

        // Remove old entry if key already exists.
        if let Some(old) = self.cache.pop(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len() as u64);
        }

        self.cache.put(key, value);
        self.used_bytes += new_size;
    }

    fn get(&mut self, key: &str) -> Option<Arc<Bytes>> {
        self.cache.get(key).cloned()
    }
}

impl MemoryCache {
    /// Create a new `MemoryCache`.
    ///
    /// * `max_bytes` — total byte limit across all shards.
    /// * `shard_count` — number of independent shards (16 is a good default).
    pub fn new(max_bytes: u64, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let per_shard = (max_bytes / shard_count as u64).max(1);
        let shards = (0..shard_count)
            .map(|_| Mutex::new(Shard::new(per_shard)))
            .collect();
        Self {
            shards,
            shard_count,
        }
    }

    /// Insert `value` under `key`.  Evicts LRU entries in the key's shard if
    /// necessary to stay within the per-shard byte budget.
    pub fn insert(&self, key: impl Into<String>, value: Bytes) {
        let key = key.into();
        let idx = self.shard_index(&key);
        self.shards[idx].lock().insert(key, Arc::new(value));
    }

    /// Return the cached value for `key`, or `None` on a miss.
    ///
    /// A hit promotes `key` to most-recently-used within its shard.
    pub fn get(&self, key: &str) -> Option<Arc<Bytes>> {
        let idx = self.shard_index(key);
        self.shards[idx].lock().get(key)
    }

    /// Remove `key` from the cache (e.g. when the backing file is deleted).
    pub fn remove(&self, key: &str) {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();
        if let Some(evicted) = shard.cache.pop(key) {
            shard.used_bytes = shard.used_bytes.saturating_sub(evicted.len() as u64);
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cache(max_bytes: u64, shards: usize) -> MemoryCache {
        MemoryCache::new(max_bytes, shards)
    }

    #[test]
    fn test_insert_and_get_hit() {
        let cache = make_cache(1024, 4);
        cache.insert("file1", Bytes::from_static(b"hello"));
        let result = cache.get("file1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), b"hello" as &[u8]);
    }

    #[test]
    fn test_miss_returns_none() {
        let cache = make_cache(1024, 4);
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_eviction_when_over_budget() {
        // Each shard budget = 10 bytes / 1 shard = 10 bytes.
        // We use single shard and 3 values of 5 bytes each.
        // After inserting the third, the first should be evicted.
        let cache = make_cache(10, 1);

        // Force all three to land in shard 0 by using a single shard.
        cache.insert("a", Bytes::from(vec![0u8; 5]));
        cache.insert("b", Bytes::from(vec![0u8; 5]));
        // "a" should be evicted to make room for "c".
        cache.insert("c", Bytes::from(vec![0u8; 5]));

        assert!(cache.get("c").is_some(), "c must be present");
        assert!(
            cache.get("b").is_some(),
            "b must be present (most recent before c)"
        );
        assert!(cache.get("a").is_none(), "a must be evicted (LRU)");
    }

    #[test]
    fn test_remove() {
        let cache = make_cache(1024, 4);
        cache.insert("k", Bytes::from_static(b"data"));
        cache.remove("k");
        assert!(cache.get("k").is_none());
    }

    #[test]
    fn test_overwrite_updates_used_bytes() {
        let cache = make_cache(1024, 1);
        cache.insert("k", Bytes::from(vec![0u8; 100]));
        cache.insert("k", Bytes::from(vec![0u8; 50]));
        let shard = cache.shards[0].lock();
        assert_eq!(shard.used_bytes, 50, "used_bytes must reflect overwrite");
    }

    #[test]
    fn test_multiple_shards_distribute_load() {
        let cache = make_cache(1024 * 1024, 16);
        for i in 0..100u32 {
            let key = format!("file_{i}");
            cache.insert(&key, Bytes::from(format!("data_{i}")));
        }
        for i in 0..100u32 {
            let key = format!("file_{i}");
            assert!(cache.get(&key).is_some(), "key {key} should be in cache");
        }
    }
}
