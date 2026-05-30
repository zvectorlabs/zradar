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
