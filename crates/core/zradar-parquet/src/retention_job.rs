//! RetentionJob — background job that deletes expired Parquet files.
//!
//! Periodically scans the `file_list` table for files whose max timestamp is
//! older than the configured retention window, deletes them from storage (both
//! local and S3 as applicable), and removes the entries from the database.
//!
//! Dependencies are held as `Arc<dyn Trait>` so the storage backend is
//! swappable without changing this code.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zradar_models::{FileListFilter, ParquetStorageConfig};
use zradar_traits::{BlockStorage, FileListRepository};

/// Deletes expired Parquet files on a configurable schedule.
pub struct RetentionJob {
    file_list_repo: Arc<dyn FileListRepository>,
    /// Optional S3 storage; when present, S3 files are deleted before
    /// removing the database entry.
    block_storage: Option<Arc<dyn BlockStorage>>,
    config: ParquetStorageConfig,
}

impl RetentionJob {
    /// Create a new `RetentionJob` (local-only, no S3 deletion).
    pub fn new(file_list_repo: Arc<dyn FileListRepository>, config: ParquetStorageConfig) -> Self {
        Self {
            file_list_repo,
            block_storage: None,
            config,
        }
    }

    /// Create a `RetentionJob` that also deletes S3 objects.
    pub fn with_storage(
        file_list_repo: Arc<dyn FileListRepository>,
        block_storage: Arc<dyn BlockStorage>,
        config: ParquetStorageConfig,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage: Some(block_storage),
            config,
        }
    }

    /// Run the retention loop until `cancel` is cancelled.
    pub async fn run(self, cancel: CancellationToken) {
        info!(
            interval_secs = self.config.retention_check_interval_secs,
            retention_days = self.config.retention_days,
            "RetentionJob started"
        );

        let interval = Duration::from_secs(self.config.retention_check_interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("RetentionJob shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_cycle().await {
                        error!(error = %e, "RetentionJob cycle failed");
                    }
                }
            }
        }
    }

    /// Scan for and delete all files older than `retention_days`.
    async fn run_cycle(&self) -> anyhow::Result<()> {
        let retention_us = (self.config.retention_days as i64) * 86_400 * 1_000_000;
        let cutoff_us = chrono::Utc::now().timestamp_micros() - retention_us;

        // Fetch all non-deleted files whose max_ts is before the cutoff.
        let all_files = self
            .file_list_repo
            .query_files(FileListFilter {
                deleted: Some(false),
                time_range_end: Some(cutoff_us),
                ..FileListFilter::default()
            })
            .await?;

        if all_files.is_empty() {
            return Ok(());
        }

        info!(
            count = all_files.len(),
            cutoff_us, "RetentionJob: deleting expired files"
        );

        let mut ids_to_delete: Vec<i64> = Vec::with_capacity(all_files.len());

        for file in &all_files {
            // Delete physical file (local or S3).
            if file.location == "s3" {
                if let Some(storage) = &self.block_storage {
                    let key = extract_s3_key(&file.file_path);
                    if let Err(e) = storage.delete(key).await {
                        warn!(
                            file_id = file.id,
                            key,
                            error = %e,
                            "RetentionJob: failed to delete S3 object"
                        );
                        // Don't remove the DB entry if physical delete failed.
                        continue;
                    }
                }
            } else {
                // Local file
                if let Err(e) = tokio::fs::remove_file(&file.file_path).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(
                        file_id = file.id,
                        path = %file.file_path,
                        error = %e,
                        "RetentionJob: failed to delete local file"
                    );
                    continue;
                }
            }

            ids_to_delete.push(file.id);
        }

        if !ids_to_delete.is_empty() {
            self.file_list_repo.delete_entries(&ids_to_delete).await?;
            info!(
                count = ids_to_delete.len(),
                "RetentionJob: deleted expired entries"
            );
        }

        Ok(())
    }
}

/// Strip `s3://bucket/` prefix from an S3 URL.
fn extract_s3_key(s3_url: &str) -> &str {
    if let Some(without_scheme) = s3_url.strip_prefix("s3://")
        && let Some(slash_pos) = without_scheme.find('/')
    {
        return &without_scheme[slash_pos + 1..];
    }
    s3_url
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use uuid::Uuid;
    use zradar_models::{FileListEntry, NewFileListEntry, StreamStats, StreamStatsUpdate};

    struct EmptyRepo;

    #[async_trait::async_trait]
    impl FileListRepository for EmptyRepo {
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
    async fn test_retention_job_no_files_does_nothing() {
        let job = RetentionJob::new(Arc::new(EmptyRepo), ParquetStorageConfig::default());
        job.run_cycle().await.unwrap();
    }

    #[tokio::test]
    async fn test_retention_job_cancel_stops_loop() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let job = RetentionJob::new(
            Arc::new(EmptyRepo),
            ParquetStorageConfig {
                retention_check_interval_secs: 3600,
                ..ParquetStorageConfig::default()
            },
        );

        cancel_clone.cancel();
        tokio::time::timeout(Duration::from_secs(1), job.run(cancel))
            .await
            .expect("RetentionJob should stop when cancelled");
    }

    #[test]
    fn test_extract_s3_key() {
        assert_eq!(
            extract_s3_key("s3://my-bucket/data/file.parquet"),
            "data/file.parquet"
        );
        assert_eq!(extract_s3_key("plain/key"), "plain/key");
    }
}
