// FileReclaimer — the single lease-aware physical-deletion chokepoint.
//
// Every path that retires a Parquet file (`CleanupJob` for retention expiry,
// `Compactor` for merged originals) only *soft-deletes* it (`deleted=true` in
// `file_list`). This job is the **only** component that:
//
// 1. Lists `deleted=true` rows,
// 2. Skips any file with an active read lease,
// 3. Removes the bytes from local disk or S3,
// 4. Atomically records storage-usage deltas and hard-deletes the metadata row.
//
// Centralizing physical removal here eliminates the delete-while-reading race
// that existed when `CleanupJob` unlinked files without consulting the lease
// registry, and it reclaims compactor orphans that were previously never
// unlinked.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zradar_models::FileListFilter;
use zradar_traits::{
    BlockStorage, FileLeaseRegistry, FileListRepository, StorageUsageDelta, StorageUsageRepository,
};

/// Statistics returned after one reclaimer cycle.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReclaimStats {
    /// Files physically removed from storage and hard-deleted from `file_list`.
    pub files_reclaimed: u64,
    /// On-disk / S3 bytes removed this cycle.
    pub bytes_freed: u64,
    /// Soft-deleted files skipped because a query still holds a read lease.
    pub files_skipped_leased: u64,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

/// Background job that physically reclaims soft-deleted Parquet files.
pub struct FileReclaimer {
    file_list_repo: Arc<dyn FileListRepository>,
    block_storage: Option<Arc<dyn BlockStorage>>,
    storage_usage_repo: Option<Arc<dyn StorageUsageRepository>>,
    lease_registry: Arc<FileLeaseRegistry>,
    interval_secs: u64,
}

impl FileReclaimer {
    /// Create a reclaimer for local-only deployments (no S3 block storage).
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        lease_registry: Arc<FileLeaseRegistry>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage: None,
            storage_usage_repo: None,
            lease_registry,
            interval_secs,
        }
    }

    /// Create a reclaimer that also deletes S3 objects.
    pub fn with_storage(
        file_list_repo: Arc<dyn FileListRepository>,
        block_storage: Arc<dyn BlockStorage>,
        lease_registry: Arc<FileLeaseRegistry>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            block_storage: Some(block_storage),
            storage_usage_repo: None,
            lease_registry,
            interval_secs,
        }
    }

    pub fn with_storage_usage_repository(
        mut self,
        storage_usage_repo: Arc<dyn StorageUsageRepository>,
    ) -> Self {
        self.storage_usage_repo = Some(storage_usage_repo);
        self
    }

    /// Execute one reclaim cycle for all tenants/projects.
    pub async fn run_now(&self) -> anyhow::Result<ReclaimStats> {
        self.run_now_scoped(None).await
    }

    /// Execute one reclaim cycle for an optional tenant/project scope.
    pub async fn run_now_scoped(
        &self,
        workspace_id: Option<uuid::Uuid>,
    ) -> anyhow::Result<ReclaimStats> {
        let started = Instant::now();
        let mut stats = ReclaimStats::default();

        let deleted_files = self
            .file_list_repo
            .query_files(FileListFilter {
                workspace_id,
                deleted: Some(true),
                ..FileListFilter::default()
            })
            .await?;

        if deleted_files.is_empty() {
            return Ok(stats);
        }

        info!(
            count = deleted_files.len(),
            "FileReclaimer: sweeping soft-deleted files"
        );

        let reclaim_day = chrono::Utc::now().date_naive();
        let mut by_project: HashMap<uuid::Uuid, Vec<_>> = HashMap::new();
        for file in deleted_files {
            by_project
                .entry(file.workspace_id.into())
                .or_default()
                .push(file);
        }

        for (workspace_id, files) in &by_project {
            let mut ids_to_delete: Vec<i64> = Vec::new();
            let mut cleanup_deltas: HashMap<
                (uuid::Uuid, String, chrono::NaiveDate),
                StorageUsageDelta,
            > = HashMap::new();

            for file in files {
                if self.lease_registry.is_leased(file.id) {
                    info!(
                        file_id = file.id,
                        path = %file.file_path,
                        "FileReclaimer: skipping leased file, will retry next cycle"
                    );
                    stats.files_skipped_leased += 1;
                    continue;
                }

                if !Self::delete_physical_file(file, self.block_storage.as_deref()).await {
                    let msg = format!(
                        "physical delete failed for soft-deleted file {} ({})",
                        file.id, file.file_path
                    );
                    warn!("{}", msg);
                    stats.errors.push(msg);
                    continue;
                }

                stats.bytes_freed += file.compressed_size.max(0) as u64;
                stats.files_reclaimed += 1;
                ids_to_delete.push(file.id);

                let key = (file.workspace_id, file.signal_type.clone(), reclaim_day);
                let delta = cleanup_deltas
                    .entry((key.0.into(), key.1.clone(), key.2))
                    .or_insert_with(|| StorageUsageDelta {
                        workspace_id: file.workspace_id,
                        signal_kind: file.signal_type.clone(),
                        day: reclaim_day,
                        compressed_bytes: 0,
                        file_count: 0,
                    });
                delta.compressed_bytes += file.compressed_size.max(0);
                delta.file_count += 1;
            }

            if ids_to_delete.is_empty() {
                continue;
            }

            let deltas = cleanup_deltas.values().cloned().collect::<Vec<_>>();
            if let Some(storage_usage_repo) = &self.storage_usage_repo {
                if let Err(e) = storage_usage_repo
                    .record_cleanup_and_delete(&deltas, &ids_to_delete)
                    .await
                {
                    let msg =
                        format!("atomic reclaim commit failed for workspace {workspace_id}: {e}");
                    error!("{}", msg);
                    stats.errors.push(msg);
                    continue;
                }
            } else if let Err(e) = self.file_list_repo.delete_entries(&ids_to_delete).await {
                let msg = format!("DB delete_entries failed during reclaim: {e}");
                error!("{}", msg);
                stats.errors.push(msg);
            }
        }

        stats.duration_ms = started.elapsed().as_millis() as u64;

        info!(
            files_reclaimed = stats.files_reclaimed,
            files_skipped_leased = stats.files_skipped_leased,
            bytes_freed = stats.bytes_freed,
            duration_ms = stats.duration_ms,
            "FileReclaimer: cycle complete"
        );

        Ok(stats)
    }

    /// Run the reclaim loop until `cancel` is cancelled.
    pub async fn run(&self, cancel: CancellationToken) {
        info!(interval_secs = self.interval_secs, "FileReclaimer started");

        let interval = Duration::from_secs(self.interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("FileReclaimer shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_now().await {
                        error!(error = %e, "FileReclaimer cycle failed");
                    }
                }
            }
        }
    }

    /// Remove bytes from storage. Returns `true` when it is safe to hard-delete
    /// the metadata row (delete succeeded or object was already gone).
    async fn delete_physical_file(
        file: &zradar_models::FileListEntry,
        block_storage: Option<&dyn BlockStorage>,
    ) -> bool {
        if file.location == "s3" {
            if let Some(storage) = block_storage {
                let key = extract_s3_key(&file.file_path);
                match storage.delete(key).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(
                            file_id = file.id,
                            key,
                            error = %e,
                            "FileReclaimer: S3 delete failed"
                        );
                        false
                    }
                }
            } else {
                // No S3 backend configured — metadata says s3 but we cannot
                // reach the object. Hard-delete the row so it does not block
                // forever; ops must reconcile orphaned S3 keys out of band.
                warn!(
                    file_id = file.id,
                    path = %file.file_path,
                    "FileReclaimer: S3 file with no BlockStorage configured; dropping metadata only"
                );
                true
            }
        } else if let Err(e) = tokio::fs::remove_file(&file.file_path).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                true
            } else {
                warn!(
                    file_id = file.id,
                    path = %file.file_path,
                    error = %e,
                    "FileReclaimer: local delete failed"
                );
                false
            }
        } else {
            true
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
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use std::sync::Mutex;

    use zradar_models::{FileListEntry, NewFileListEntry, StreamStats, StreamStatsUpdate};

    #[allow(dead_code)]
    struct MockBlockStorage {
        deleted: Mutex<Vec<String>>,
    }

    #[allow(dead_code)]
    impl MockBlockStorage {
        fn new() -> Self {
            Self {
                deleted: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl BlockStorage for MockBlockStorage {
        async fn upload(&self, _key: &str, _data: &[u8]) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn download(&self, _key: &str) -> anyhow::Result<Vec<u8>> {
            Ok(vec![])
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.deleted.lock().unwrap().push(key.to_string());
            Ok(())
        }
        async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn cleanup(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct ReclaimRepo {
        files: Vec<FileListEntry>,
        deleted_ids: Mutex<Vec<i64>>,
    }

    #[async_trait::async_trait]
    impl FileListRepository for ReclaimRepo {
        async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
            Ok(1)
        }
        async fn query_files(&self, filter: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(self
                .files
                .iter()
                .filter(|f| filter.deleted.is_none_or(|deleted| f.deleted == deleted))
                .cloned()
                .collect())
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, ids: &[i64]) -> anyhow::Result<()> {
            self.deleted_ids.lock().unwrap().extend_from_slice(ids);
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

    fn make_deleted_entry(tmp: &tempfile::TempDir, id: i64) -> FileListEntry {
        let path = tmp.path().join(format!("file_{id}.parquet"));
        std::fs::write(&path, b"parquet").unwrap();
        FileListEntry {
            id,
            workspace_id: WorkspaceId::from(uuid::Uuid::nil()),
            signal_type: "traces".to_string(),
            stream_name: "default".to_string(),
            date: "2026/06/26/00".to_string(),
            file_path: path.to_string_lossy().into_owned(),
            location: "local".to_string(),
            min_ts: 0,
            max_ts: 1,
            records: 1,
            original_size: 7,
            compressed_size: 7,
            deleted: true,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn reclaimer_physically_removes_soft_deleted_local_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let entry = make_deleted_entry(&tmp, 10);
        let path = entry.file_path.clone();
        let repo = Arc::new(ReclaimRepo {
            files: vec![entry],
            deleted_ids: Mutex::new(Vec::new()),
        });
        let registry = Arc::new(FileLeaseRegistry::new());

        let reclaimer = FileReclaimer::new(repo.clone(), registry, 3600);
        let stats = reclaimer.run_now().await.unwrap();

        assert_eq!(stats.files_reclaimed, 1);
        assert_eq!(stats.files_skipped_leased, 0);
        assert!(!std::path::Path::new(&path).exists());
        assert_eq!(repo.deleted_ids.lock().unwrap().as_slice(), &[10]);
    }

    #[tokio::test]
    async fn reclaimer_skips_leased_file_and_retries_after_release() {
        let tmp = tempfile::TempDir::new().unwrap();
        let entry = make_deleted_entry(&tmp, 42);
        let path = entry.file_path.clone();
        let repo = Arc::new(ReclaimRepo {
            files: vec![entry],
            deleted_ids: Mutex::new(Vec::new()),
        });
        let registry = Arc::new(FileLeaseRegistry::new());
        let reclaimer = FileReclaimer::new(repo.clone(), registry.clone(), 3600);

        let _lease = registry.acquire(&[42]);
        let stats = reclaimer.run_now().await.unwrap();
        assert_eq!(stats.files_reclaimed, 0);
        assert_eq!(stats.files_skipped_leased, 1);
        assert!(std::path::Path::new(&path).exists());
        assert!(repo.deleted_ids.lock().unwrap().is_empty());

        drop(_lease);
        let stats = reclaimer.run_now().await.unwrap();
        assert_eq!(stats.files_reclaimed, 1);
        assert!(!std::path::Path::new(&path).exists());
        assert_eq!(repo.deleted_ids.lock().unwrap().as_slice(), &[42]);
    }

    #[tokio::test]
    async fn reclaimer_reclaims_compactor_orphan() {
        // Simulates a file soft-deleted by Compactor (`deleted=true`) that
        // CleanupJob never saw because it only listed `deleted=false`.
        let tmp = tempfile::TempDir::new().unwrap();
        let entry = make_deleted_entry(&tmp, 99);
        let path = entry.file_path.clone();
        let repo = Arc::new(ReclaimRepo {
            files: vec![entry],
            deleted_ids: Mutex::new(Vec::new()),
        });
        let registry = Arc::new(FileLeaseRegistry::new());
        let reclaimer = FileReclaimer::new(repo.clone(), registry, 3600);

        let stats = reclaimer.run_now().await.unwrap();
        assert_eq!(stats.files_reclaimed, 1);
        assert!(!std::path::Path::new(&path).exists());
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
