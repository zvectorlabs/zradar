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
use zradar_traits::{BlockStorage, FileLeaseRegistry, FileListRepository};

/// Moves local Parquet files to S3 on a configurable schedule.
pub struct FileMover {
    file_list_repo: Arc<dyn FileListRepository>,
    block_storage: Arc<dyn BlockStorage>,
    config: ParquetStorageConfig,
    data_dir: PathBuf,
    lease_registry: Option<Arc<FileLeaseRegistry>>,
}

impl FileMover {
    /// Create a new `FileMover`.
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        block_storage: Arc<dyn BlockStorage>,
        config: ParquetStorageConfig,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage,
            config,
            data_dir,
            lease_registry: None,
        }
    }

    /// Install a [`FileLeaseRegistry`] so that files actively being read by
    /// queries are skipped this tick. They will be retried on the next cycle.
    pub fn with_lease_registry(mut self, registry: Arc<FileLeaseRegistry>) -> Self {
        self.lease_registry = Some(registry);
        self
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
            .filter(|f| {
                // Defer files that an active query is reading. Logged so that
                // a chronically-leased file is visible in ops without spamming.
                match &self.lease_registry {
                    Some(registry) if registry.is_leased(f.id) => {
                        info!(
                            file_id = f.id,
                            path = %f.file_path,
                            "FileMover: skipping leased file, will retry next cycle"
                        );
                        false
                    }
                    _ => true,
                }
            })
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

            let s3_key = s3_key_for_local_file(&self.data_dir, &local_path);

            match self.block_storage.upload(&s3_key, &data).await {
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

fn s3_key_for_local_file(data_dir: &std::path::Path, local_path: &std::path::Path) -> String {
    let data_dir_key = path_to_s3_key(data_dir);
    let relative = local_path
        .strip_prefix(data_dir)
        .ok()
        .map(path_to_s3_key)
        .filter(|key| !key.is_empty());

    let Some(relative_key) = relative else {
        return path_to_s3_key(local_path);
    };

    let data_dir_basename = data_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let duplicate_prefix = format!("{data_dir_basename}/");
    let normalized_relative = relative_key
        .strip_prefix(&duplicate_prefix)
        .unwrap_or(&relative_key);

    if data_dir_key.is_empty() {
        normalized_relative.to_string()
    } else {
        format!("{data_dir_key}/{normalized_relative}")
    }
}

fn path_to_s3_key(path: &std::path::Path) -> String {
    path.to_string_lossy()
        .trim_start_matches("./")
        .trim_start_matches('/')
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use std::sync::Mutex;

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
        async fn get_stream_stats(
            &self,
            _: zradar_models::WorkspaceId,
        ) -> anyhow::Result<Vec<StreamStats>> {
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
            PathBuf::from("/tmp/zradar/files"),
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
            PathBuf::from("/tmp/zradar/files"),
        );

        // Cancel immediately
        cancel_clone.cancel();
        // run() should return without blocking
        tokio::time::timeout(Duration::from_secs(1), mover.run(cancel))
            .await
            .expect("FileMover should stop when cancelled");
    }

    /// Repo that returns one eligible local file. Used to drive the lease
    /// skip path without standing up Postgres.
    struct SingleFileRepo {
        entry: FileListEntry,
        update_calls: Mutex<Vec<i64>>,
    }

    #[async_trait::async_trait]
    impl FileListRepository for SingleFileRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            Ok(self.entry.id)
        }
        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(vec![self.entry.clone()])
        }
        async fn update_location(
            &self,
            file_id: i64,
            _location: &str,
            _path: &str,
        ) -> anyhow::Result<()> {
            self.update_calls.lock().unwrap().push(file_id);
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(
            &self,
            _: zradar_models::WorkspaceId,
        ) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn make_entry(tmp: &tempfile::TempDir, id: i64) -> FileListEntry {
        let path = tmp.path().join(format!("file_{id}.parquet"));
        // Touch the file so FileMover's tokio::fs::read does not skip it.
        std::fs::write(&path, b"parquet bytes here").unwrap();
        FileListEntry {
            id,
            workspace_id: WorkspaceId::from(uuid::Uuid::nil()),
            signal_type: "traces".to_string(),
            stream_name: "default".to_string(),
            date: "2026/06/26/00".to_string(),
            file_path: path.to_string_lossy().into_owned(),
            location: "local".to_string(),
            min_ts: 0,
            max_ts: i64::MAX,
            records: 1,
            original_size: 0,
            compressed_size: 0,
            deleted: false,
            // Created far enough in the past to clear `file_push_delay_secs`.
            created_at: 0,
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn file_mover_uploads_when_no_lease() {
        let tmp = tempfile::TempDir::new().unwrap();
        let entry = make_entry(&tmp, 1);
        let repo = Arc::new(SingleFileRepo {
            entry,
            update_calls: Mutex::new(Vec::new()),
        });
        let storage = Arc::new(MockBlockStorage::new());

        let mover = FileMover::new(
            repo.clone(),
            storage.clone(),
            ParquetStorageConfig::default(),
            tmp.path().to_path_buf(),
        );

        mover.push_eligible_files().await.unwrap();

        // The file was uploaded and its location updated.
        assert_eq!(storage.uploaded.lock().unwrap().len(), 1);
        assert_eq!(repo.update_calls.lock().unwrap().as_slice(), &[1]);
    }

    #[tokio::test]
    async fn file_mover_skips_leased_file_and_resumes_after_release() {
        let tmp = tempfile::TempDir::new().unwrap();
        let entry = make_entry(&tmp, 42);
        let repo = Arc::new(SingleFileRepo {
            entry,
            update_calls: Mutex::new(Vec::new()),
        });
        let storage = Arc::new(MockBlockStorage::new());
        let registry = Arc::new(FileLeaseRegistry::new());

        let mover = FileMover::new(
            repo.clone(),
            storage.clone(),
            ParquetStorageConfig::default(),
            tmp.path().to_path_buf(),
        )
        .with_lease_registry(registry.clone());

        // Acquire a lease — simulating an in-flight query reading the file.
        let _lease = registry.acquire(&[42]);
        mover.push_eligible_files().await.unwrap();
        assert!(
            storage.uploaded.lock().unwrap().is_empty(),
            "leased file must not be uploaded"
        );
        assert!(
            repo.update_calls.lock().unwrap().is_empty(),
            "leased file must not have its location updated"
        );

        // Release the lease and run again — now the upload proceeds.
        drop(_lease);
        mover.push_eligible_files().await.unwrap();
        assert_eq!(storage.uploaded.lock().unwrap().len(), 1);
        assert_eq!(repo.update_calls.lock().unwrap().as_slice(), &[42]);
    }

    #[test]
    fn test_s3_key_for_local_file_collapses_duplicate_data_dir_basename() {
        let key = s3_key_for_local_file(
            std::path::Path::new("/workspace/zradar-platform/files"),
            std::path::Path::new(
                "/workspace/zradar-platform/files/files/tenant/traces/service/2026/05/28/00/a.parquet",
            ),
        );

        assert_eq!(
            key,
            "workspace/zradar-platform/files/tenant/traces/service/2026/05/28/00/a.parquet"
        );
    }

    #[test]
    fn test_s3_key_for_local_file_preserves_non_duplicate_relative_prefix() {
        let key = s3_key_for_local_file(
            std::path::Path::new("/data/parquet"),
            std::path::Path::new("/data/parquet/files/tenant/traces/a.parquet"),
        );

        assert_eq!(key, "data/parquet/files/tenant/traces/a.parquet");
    }
}
