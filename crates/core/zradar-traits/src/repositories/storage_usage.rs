use async_trait::async_trait;
use chrono::NaiveDate;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUsageDelta {
    pub tenant_id: Uuid,
    pub project_id: Uuid,
    pub signal_kind: String,
    pub day: NaiveDate,
    pub compressed_bytes: i64,
    pub file_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUsageDailySnapshot {
    pub tenant_id: Uuid,
    pub project_id: Uuid,
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

    async fn snapshot_storage_daily(&self, day: NaiveDate) -> anyhow::Result<()>;

    async fn query_storage_usage_daily(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal_kind: Option<&str>,
        start_micros: Option<i64>,
        end_micros: Option<i64>,
    ) -> anyhow::Result<Vec<StorageUsageDailySnapshot>>;
}
