//! Local filesystem block storage implementation

use async_trait::async_trait;
use std::path::PathBuf;
use zradar_traits::BlockStorage;

/// Local filesystem block storage (for development/testing)
pub struct LocalBlockStorage {
    base_path: PathBuf,
}

impl LocalBlockStorage {
    /// Create new local block storage
    ///
    /// # Arguments
    /// * `base_path` - Base directory for storage
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        let base_path = base_path.into();

        // Create base directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&base_path) {
            tracing::warn!(
                error = %e,
                path = ?base_path,
                "Failed to create base directory (may already exist)"
            );
        }

        Self { base_path }
    }

    /// Get the base path
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }
}

#[async_trait]
impl BlockStorage for LocalBlockStorage {
    async fn upload(&self, key: &str, data: &[u8]) -> anyhow::Result<String> {
        let file_path = self.base_path.join(key);

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&file_path, data).await?;

        tracing::debug!(
            key = key,
            size = data.len(),
            path = ?file_path,
            "Uploaded to local storage"
        );

        // Return the key (not full path) so download() can reconstruct it
        Ok(key.to_string())
    }

    async fn download(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let file_path = self.base_path.join(key);

        let data = tokio::fs::read(&file_path).await?;

        tracing::debug!(
            key = key,
            size = data.len(),
            path = ?file_path,
            "Downloaded from local storage"
        );

        Ok(data)
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let file_path = self.base_path.join(key);

        tokio::fs::remove_file(&file_path).await?;

        tracing::debug!(
            key = key,
            path = ?file_path,
            "Deleted from local storage"
        );

        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let file_path = self.base_path.join(key);

        Ok(tokio::fs::try_exists(&file_path).await.unwrap_or(false))
    }

    async fn cleanup(&self, key: &str) -> anyhow::Result<()> {
        // For local storage: delete immediately after processing
        self.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_upload_download_delete() {
        let temp_dir = std::env::temp_dir().join("zradar-test");
        let storage = LocalBlockStorage::new(&temp_dir);

        let key = "test/file.dat";
        let data = b"test data";

        // Upload
        let path = storage.upload(key, data).await.unwrap();
        assert!(path.contains(key));

        // Exists
        assert!(storage.exists(key).await.unwrap());

        // Download
        let downloaded = storage.download(key).await.unwrap();
        assert_eq!(downloaded, data);

        // Delete
        storage.delete(key).await.unwrap();
        assert!(!storage.exists(key).await.unwrap());
    }
}
