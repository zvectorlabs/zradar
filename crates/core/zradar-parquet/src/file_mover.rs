//! FileMover — background job that promotes local Parquet files to S3.
//!
//! When S3 is configured, the FileMover periodically scans the `file_list`
//! table for files that are stored locally and old enough to be pushed.
//! Each file is:
//! 1. Read from local disk and uploaded to S3 via `BlockStorage`.
//! 2. Updated in `file_list` with location = "s3" and the new S3 key.
//! 3. Scheduled for local deletion after `file_delete_local_delay_secs`.
//!
//! All dependencies are held as `Arc<dyn Trait>` so the storage backend
//! can be swapped without changing this code.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zradar_models::{FileListFilter, ParquetStorageConfig};
use zradar_traits::{BlockStorage, FileListRepository};

/// Moves local Parquet files to S3 on a configurable schedule.
pub struct FileMover {
    file_list_repo: Arc<dyn FileListRepository>,
    block_storage: Arc<dyn BlockStorage>,
    config: ParquetStorageConfig,
}

impl FileMover {
    /// Create a new `FileMover`.
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        block_storage: Arc<dyn BlockStorage>,
        config: ParquetStorageConfig,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage,
            config,
        }
    }

    /// Run the FileMover loop until `cancel` is cancelled.
    ///
    /// Wakes up every `file_push_interval_secs` seconds to move eligible files.
    pub async fn run(self, cancel: CancellationToken) {
        info!(
            interval_secs = self.config.file_push_interval_secs,
            delay_secs = self.config.file_push_delay_secs,
            "FileMover started"
        );

        let interval = Duration::from_secs(self.config.file_push_interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("FileMover shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.push_eligible_files().await {
                        error!(error = %e, "FileMover push cycle failed");
                    }
                }
            }
        }
    }

    /// Push all local files that are old enough to be promoted to S3.
    async fn push_eligible_files(&self) -> anyhow::Result<()> {
        let now_us = chrono::Utc::now().timestamp_micros();
        let delay_us = (self.config.file_push_delay_secs * 1_000_000) as i64;
        let cutoff_us = now_us - delay_us;

        // Query all non-deleted local files
        let filter = FileListFilter {
            location: Some("local".to_string()),
            deleted: Some(false),
            ..FileListFilter::default()
        };

        let files = self.file_list_repo.query_files(filter).await?;

        let eligible: Vec<_> = files
            .into_iter()
            .filter(|f| f.created_at <= cutoff_us)
            .collect();

        if eligible.is_empty() {
            return Ok(());
        }

        info!(count = eligible.len(), "FileMover: pushing files to S3");

        for file in eligible {
            let local_path = PathBuf::from(&file.file_path);

            let data = match tokio::fs::read(&local_path).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(
                        file_id = file.id,
                        path = %file.file_path,
                        error = %e,
                        "FileMover: failed to read local file, skipping"
                    );
                    continue;
                }
            };

            // S3 key mirrors the local path structure (strip leading './')
            let s3_key = file
                .file_path
                .trim_start_matches("./")
                .trim_start_matches('/');

            match self.block_storage.upload(s3_key, &data).await {
                Ok(s3_url) => {
                    self.file_list_repo
                        .update_location(file.id, "s3", &s3_url)
                        .await?;

                    info!(
                        file_id = file.id,
                        s3_url = %s3_url,
                        "FileMover: promoted file to S3"
                    );

                    // Schedule local deletion by spawning a delayed task.
                    let delay = Duration::from_secs(self.config.file_delete_local_delay_secs);
                    let path_clone = local_path.clone();
                    let file_id = file.id;
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        if let Err(e) = tokio::fs::remove_file(&path_clone).await {
                            warn!(
                                file_id = file_id,
                                path = %path_clone.display(),
                                error = %e,
                                "FileMover: failed to delete local file after S3 upload"
                            );
                        } else {
                            info!(
                                file_id = file_id,
                                path = %path_clone.display(),
                                "FileMover: deleted local copy after S3 promotion"
                            );
                        }
                    });
                }
                Err(e) => {
                    error!(
                        file_id = file.id,
                        path = %file.file_path,
                        error = %e,
                        "FileMover: S3 upload failed"
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;
    use zradar_models::{FileListEntry, NewFileListEntry, StreamStats, StreamStatsUpdate};

    struct MockBlockStorage {
        uploaded: Mutex<Vec<String>>,
    }

    impl MockBlockStorage {
        fn new() -> Self {
            Self {
                uploaded: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait::async_trait]
    impl BlockStorage for MockBlockStorage {
        async fn upload(&self, key: &str, _data: &[u8]) -> anyhow::Result<String> {
            self.uploaded.lock().unwrap().push(key.to_string());
            Ok(format!("s3://test-bucket/{}", key))
        }
        async fn download(&self, _key: &str) -> anyhow::Result<Vec<u8>> {
            Ok(vec![])
        }
        async fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn cleanup(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct EmptyFileListRepo;

    #[async_trait::async_trait]
    impl FileListRepository for EmptyFileListRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            Ok(1)
        }
        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(vec![])
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(&self, _: Uuid, _: Uuid) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_file_mover_no_files_does_nothing() {
        let mover = FileMover::new(
            Arc::new(EmptyFileListRepo),
            Arc::new(MockBlockStorage::new()),
            ParquetStorageConfig::default(),
        );

        // Should succeed with no eligible files
        mover.push_eligible_files().await.unwrap();
    }

    #[tokio::test]
    async fn test_file_mover_cancel_stops_loop() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let mover = FileMover::new(
            Arc::new(EmptyFileListRepo),
            Arc::new(MockBlockStorage::new()),
            ParquetStorageConfig {
                file_push_interval_secs: 3600, // long interval — cancel fires first
                ..ParquetStorageConfig::default()
            },
        );

        // Cancel immediately
        cancel_clone.cancel();
        // run() should return without blocking
        tokio::time::timeout(Duration::from_secs(1), mover.run(cancel))
            .await
            .expect("FileMover should stop when cancelled");
    }
}
