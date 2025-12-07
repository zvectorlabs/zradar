//! Block storage trait definitions for raw telemetry data
//!
//! Provides abstraction over different storage backends:
//! - S3 (production)
//! - Local filesystem (development)
//! - In-memory (testing)

use async_trait::async_trait;

/// Abstract block storage interface
///
/// Implementations:
/// - S3BlockStorage (production)
/// - LocalBlockStorage (development)
/// - MemoryBlockStorage (testing)
#[async_trait]
pub trait BlockStorage: Send + Sync + 'static {
    /// Upload data block
    ///
    /// # Arguments
    /// * `key` - Storage key (path)
    /// * `data` - Raw data bytes
    ///
    /// # Returns
    /// Full storage path/URL
    async fn upload(&self, key: &str, data: &[u8]) -> anyhow::Result<String>;

    /// Download data block
    ///
    /// # Arguments
    /// * `key` - Storage key (path)
    ///
    /// # Returns
    /// Raw data bytes
    async fn download(&self, key: &str) -> anyhow::Result<Vec<u8>>;

    /// Delete data block
    ///
    /// # Arguments
    /// * `key` - Storage key (path)
    async fn delete(&self, key: &str) -> anyhow::Result<()>;

    /// Check if block exists
    ///
    /// # Arguments
    /// * `key` - Storage key (path)
    ///
    /// # Returns
    /// true if block exists, false otherwise
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;

    /// Cleanup after successful processing
    ///
    /// For local storage: Deletes the file immediately
    /// For S3: No-op (relies on lifecycle policies for auto-deletion after 24h)
    ///
    /// # Arguments
    /// * `key` - Storage key (path) to clean up
    async fn cleanup(&self, key: &str) -> anyhow::Result<()>;
}
