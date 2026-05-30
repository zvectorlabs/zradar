use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use zradar_traits::StorageUsageRepository;

pub struct StorageUsageDailyJob {
    storage_usage_repo: Arc<dyn StorageUsageRepository>,
    interval_secs: u64,
}

impl StorageUsageDailyJob {
    pub fn new(storage_usage_repo: Arc<dyn StorageUsageRepository>, interval_secs: u64) -> Self {
        Self {
            storage_usage_repo,
            interval_secs,
        }
    }

    pub async fn run_now(&self, day: NaiveDate) -> anyhow::Result<()> {
        self.storage_usage_repo.snapshot_storage_daily(day).await
    }

    pub async fn run(&self, cancel: CancellationToken) {
        info!(
            interval_secs = self.interval_secs,
            "StorageUsageDailyJob started"
        );

        let interval = Duration::from_secs(self.interval_secs);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("StorageUsageDailyJob shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
                    if let Err(e) = self.run_now(day).await {
                        error!(error = %e, "StorageUsageDailyJob cycle failed");
                    }
                }
            }
        }
    }
}
