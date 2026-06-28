use async_trait::async_trait;
use chrono::NaiveDate;
use zradar_models::WorkspaceId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUsageDelta {
    pub workspace_id: WorkspaceId,
    pub signal_kind: String,
    pub day: NaiveDate,
    pub compressed_bytes: i64,
    pub file_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUsageDailySnapshot {
    pub workspace_id: WorkspaceId,
    pub signal_kind: String,
    pub day: String,
    pub compressed_bytes: i64,
    pub file_count: i64,
    pub captured_at: i64,
    pub estimated_today: bool,
}

#[async_trait]
pub trait StorageUsageRepository: Send + Sync {
    async fn record_cleanup_daily(&self, deltas: &[StorageUsageDelta]) -> anyhow::Result<()>;

    /// Atomically record cleanup deltas in `storage_cleanup_daily` and hard-delete
    /// the corresponding `file_list` entries in a single transaction.
    ///
    /// This prevents the split-brain where accounting succeeds but the DB delete
    /// fails (double-count on retry) or the DB delete succeeds but accounting fails
    /// (permanent undercount). Either both commit or neither does.
    ///
    /// Physical storage deletion (S3/local) must happen **before** calling this
    /// method, as it is idempotent and safe to retry on the next cleanup cycle.
    async fn record_cleanup_and_delete(
        &self,
        deltas: &[StorageUsageDelta],
        file_ids: &[i64],
    ) -> anyhow::Result<()>;

    /// Return `(compressed_bytes, file_count)` ingested for the key on `day`,
    /// or `(0, 0)` if no ingestion was recorded.
    async fn get_ingestion_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<(i64, i64)>;

    /// Return `(compressed_bytes, file_count)` removed by cleanup for the key
    /// on `day`, or `(0, 0)` if nothing was cleaned up.
    async fn get_cleanup_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<(i64, i64)>;

    /// Return the most recent snapshot row for the given key on `day`, or `None`
    /// if this is the first snapshot for that combination (bootstrap case).
    async fn get_previous_snapshot(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
    ) -> anyhow::Result<Option<(i64, i64)>>;

    /// Upsert a single snapshot row into `retention_storage_daily` (bucket 0).
    /// Idempotent: re-running for the same key+day overwrites with the new values.
    async fn upsert_storage_snapshot(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        day: NaiveDate,
        compressed_bytes: i64,
        file_count: i64,
    ) -> anyhow::Result<()>;

    /// Return the current storage size for a key, counting only files created
    /// before `before_micros` and not deleted.
    async fn get_current_file_stats(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: &str,
        before_micros: i64,
    ) -> anyhow::Result<(i64, i64)>;

    async fn query_storage_usage_daily(
        &self,
        workspace_id: WorkspaceId,
        signal_kind: Option<&str>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> anyhow::Result<Vec<StorageUsageDailySnapshot>>;
}
