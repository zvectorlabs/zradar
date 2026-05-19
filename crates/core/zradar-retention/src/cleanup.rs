//! CleanupJob — deletes Parquet files that have exceeded their retention window.
//!
//! The job queries `file_list` grouped by `(tenant_id, project_id)` and uses
//! `RetentionConfigStore` to determine the per-project cutoff.  Files whose
//! `max_ts` falls before the cutoff are deleted from storage and removed from
//! the database.
//!
//! `run_now()` executes one cleanup cycle synchronously — useful for tests and
//! admin-triggered cleanups.  `run(cancel)` loops on a configurable schedule.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zradar_models::FileListFilter;
use zradar_traits::{BlockStorage, FileListRepository};

use crate::config::RetentionConfigStore;

/// Statistics returned after a cleanup cycle.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct CleanupStats {
    pub files_deleted: u64,
    pub bytes_freed: u64,
    pub projects_processed: u32,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

/// Background job that deletes expired Parquet files.
pub struct CleanupJob {
    file_list_repo: Arc<dyn FileListRepository>,
    block_storage: Option<Arc<dyn BlockStorage>>,
    config_store: Arc<RetentionConfigStore>,
    /// How often (seconds) the scheduled loop wakes up.
    interval_secs: u64,
}

impl CleanupJob {
    /// Create a new `CleanupJob` (local-only, no S3 deletion).
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        config_store: Arc<RetentionConfigStore>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage: None,
            config_store,
            interval_secs,
        }
    }

    /// Create a `CleanupJob` that also deletes S3 objects.
    pub fn with_storage(
        file_list_repo: Arc<dyn FileListRepository>,
        block_storage: Arc<dyn BlockStorage>,
        config_store: Arc<RetentionConfigStore>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage: Some(block_storage),
            config_store,
            interval_secs,
        }
    }

    /// Execute one cleanup cycle and return statistics.
    ///
    /// This is the primary entry point for admin-triggered and test-driven
    /// cleanups.  It is also called internally by the scheduled `run` loop.
    pub async fn run_now(&self) -> anyhow::Result<CleanupStats> {
        let started = Instant::now();
        let mut stats = CleanupStats::default();

        // Fetch all non-deleted files (no time filter — we compute cutoffs per project).
        let all_files = self
            .file_list_repo
            .query_files(FileListFilter {
                deleted: Some(false),
                ..FileListFilter::default()
            })
            .await?;

        if all_files.is_empty() {
            return Ok(stats);
        }

        // Group files by (tenant_id, project_id) so we do one cutoff lookup per project.
        use std::collections::HashMap;
        let mut by_project: HashMap<(uuid::Uuid, uuid::Uuid), Vec<_>> = HashMap::new();
        for file in all_files {
            by_project
                .entry((file.tenant_id, file.project_id))
                .or_default()
                .push(file);
        }

        stats.projects_processed = by_project.len() as u32;

        for ((tenant_id, project_id), files) in &by_project {
            let cutoff_us = self.config_store.get_cutoff_us(*tenant_id, *project_id);

            let expired: Vec<_> = files.iter().filter(|f| f.max_ts <= cutoff_us).collect();

            if expired.is_empty() {
                continue;
            }

            info!(
                tenant_id = %tenant_id,
                project_id = %project_id,
                count = expired.len(),
                cutoff_us,
                "CleanupJob: deleting expired files"
            );

            let mut ids_to_delete: Vec<i64> = Vec::with_capacity(expired.len());

            for file in &expired {
                // Physical deletion
                if file.location == "s3" {
                    if let Some(storage) = &self.block_storage {
                        let key = extract_s3_key(&file.file_path);
                        if let Err(e) = storage.delete(key).await {
                            let msg = format!("S3 delete failed for file {}: {}", file.id, e);
                            warn!("{}", msg);
                            stats.errors.push(msg);
                            continue;
                        }
                    }
                } else if let Err(e) = tokio::fs::remove_file(&file.file_path).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    let msg = format!("Local delete failed for file {}: {}", file.id, e);
                    warn!("{}", msg);
                    stats.errors.push(msg);
                    continue;
                }

                stats.bytes_freed += file.compressed_size as u64;
                stats.files_deleted += 1;
                ids_to_delete.push(file.id);
            }

            if !ids_to_delete.is_empty()
                && let Err(e) = self.file_list_repo.delete_entries(&ids_to_delete).await
            {
                let msg = format!("DB delete_entries failed: {}", e);
                error!("{}", msg);
                stats.errors.push(msg);
            }
        }

        stats.duration_ms = started.elapsed().as_millis() as u64;

        info!(
            files_deleted = stats.files_deleted,
            bytes_freed = stats.bytes_freed,
            duration_ms = stats.duration_ms,
            "CleanupJob: cycle complete"
        );

        Ok(stats)
    }

    /// Run the cleanup loop until `cancel` is cancelled.
    pub async fn run(&self, cancel: CancellationToken) {
        info!(interval_secs = self.interval_secs, "CleanupJob started");

        let interval = Duration::from_secs(self.interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("CleanupJob shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_now().await {
                        error!(error = %e, "CleanupJob cycle failed");
                    }
                }
            }
        }
    }
}

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
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
    };

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
    async fn test_run_now_no_files_returns_empty_stats() {
        let store = Arc::new(RetentionConfigStore::new(30));
        let job = CleanupJob::new(Arc::new(EmptyRepo), store, 3600);
        let stats = job.run_now().await.unwrap();
        assert_eq!(stats.files_deleted, 0);
        assert!(stats.errors.is_empty());
    }

    #[tokio::test]
    async fn test_cancel_stops_scheduled_loop() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let store = Arc::new(RetentionConfigStore::new(30));
        let job = Arc::new(CleanupJob::new(Arc::new(EmptyRepo), store, 3600));

        cancel_clone.cancel();
        tokio::time::timeout(Duration::from_secs(1), job.run(cancel))
            .await
            .expect("CleanupJob should stop when cancelled");
    }

    #[test]
    fn test_extract_s3_key() {
        assert_eq!(
            extract_s3_key("s3://bucket/data/file.parquet"),
            "data/file.parquet"
        );
        assert_eq!(extract_s3_key("plain/key"), "plain/key");
    }
}
