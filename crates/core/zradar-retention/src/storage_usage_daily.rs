use std::sync::Arc;
use std::time::Duration;
use zradar_models::WorkspaceId;

use chrono::NaiveDate;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zradar_traits::{FileListRepository, StorageUsageRepository};

/// Maximum number of (workspace, signal) keys snapshotted concurrently.
const SNAPSHOT_CONCURRENCY: usize = 10;

pub struct StorageUsageDailyJob {
    file_list_repo: Arc<dyn FileListRepository>,
    storage_usage_repo: Arc<dyn StorageUsageRepository>,
    interval_secs: u64,
}

impl StorageUsageDailyJob {
    pub fn new(
        file_list_repo: Arc<dyn FileListRepository>,
        storage_usage_repo: Arc<dyn StorageUsageRepository>,
        interval_secs: u64,
    ) -> Self {
        Self {
            file_list_repo,
            storage_usage_repo,
            interval_secs,
        }
    }

    /// Snapshot storage usage for all active keys on `day`.
    ///
    /// For each (workspace, signal) combination:
    /// - Looks up the previous day's snapshot in `retention_storage_daily`.
    /// - If found (incremental): applies ingestion and cleanup deltas from the
    ///   daily accounting tables to produce the new snapshot value.
    /// - If not found (bootstrap): counts current non-deleted files in
    ///   `file_list` directly as the baseline.
    ///
    /// Keys are processed in parallel with bounded concurrency. A failure for
    /// one key is logged and skipped — it does not abort other keys.
    pub async fn run_now(&self, day: NaiveDate) -> anyhow::Result<()> {
        // Upper bound: only count files that existed by end-of-day so that
        // a backfill run for a past day is not inflated by later ingestion.
        let day_end_micros = day_end_micros(day);

        let keys = self.file_list_repo.list_active_keys(day_end_micros).await?;

        if keys.is_empty() {
            info!("StorageUsageDailyJob: no active keys for {day}, skipping");
            return Ok(());
        }

        info!(
            day = %day,
            key_count = keys.len(),
            "StorageUsageDailyJob: snapshotting"
        );

        let sem = Arc::new(Semaphore::new(SNAPSHOT_CONCURRENCY));
        let mut tasks = tokio::task::JoinSet::new();

        for (workspace_id, signal_kind) in keys {
            let permit = sem.clone().acquire_owned().await?;
            let repo = self.storage_usage_repo.clone();

            tasks.spawn(async move {
                let _permit = permit;
                snapshot_one_key(
                    repo.as_ref(),
                    workspace_id,
                    &signal_kind,
                    day,
                    day_end_micros,
                )
                .await
                .map_err(|e| format!("{workspace_id}/{signal_kind}: {e}"))
            });
        }

        let mut errors = 0usize;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(msg)) => {
                    warn!("StorageUsageDailyJob: key failed: {msg}");
                    errors += 1;
                }
                Err(join_err) => {
                    warn!("StorageUsageDailyJob: task panicked: {join_err}");
                    errors += 1;
                }
            }
        }

        if errors > 0 {
            // Return an error so the caller / scheduler knows the cycle was partial.
            anyhow::bail!("StorageUsageDailyJob: {errors} key(s) failed for {day}");
        }

        info!(day = %day, "StorageUsageDailyJob: snapshot complete");
        Ok(())
    }

    pub async fn run(&self, cancel: CancellationToken) {
        info!(
            interval_secs = self.interval_secs,
            "StorageUsageDailyJob started"
        );

        let mut interval = tokio::time::interval(Duration::from_secs(self.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("StorageUsageDailyJob shutting down");
                    return;
                }
                _ = interval.tick() => {
                    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
                    if let Err(e) = self.run_now(day).await {
                        error!(error = %e, "StorageUsageDailyJob cycle failed");
                    }
                }
            }
        }
    }
}

/// Snapshot a single (workspace, signal) key for `day`.
///
/// Logic:
///
/// 1. Load the previous day's snapshot (if any).
/// 2. If found (incremental): load ingestion added and cleanup removed for `day`,
///    apply the delta: `prev + added - removed`, floor at 0.
/// 3. If not found (bootstrap): count current non-deleted files in `file_list`
///    scoped to `before_micros` as the baseline.
/// 4. Upsert the result into `retention_storage_daily`.
async fn snapshot_one_key(
    repo: &dyn StorageUsageRepository,
    workspace_id: WorkspaceId,
    signal_kind: &str,
    day: NaiveDate,
    day_end_micros: i64,
) -> anyhow::Result<()> {
    let (compressed_bytes, file_count) = match repo
        .get_previous_snapshot(workspace_id, signal_kind, day)
        .await?
    {
        Some((prev_bytes, prev_count)) => {
            // Incremental: load today's ingestion and cleanup deltas.
            let (added_bytes, added_count) = repo
                .get_ingestion_daily(workspace_id, signal_kind, day)
                .await?;
            let (removed_bytes, removed_count) = repo
                .get_cleanup_daily(workspace_id, signal_kind, day)
                .await?;

            let bytes = (prev_bytes + added_bytes - removed_bytes).max(0);
            let count = (prev_count + added_count - removed_count).max(0);
            (bytes, count)
        }
        None => {
            // Bootstrap: full count from file_list, bounded by day_end.
            repo.get_current_file_stats(workspace_id, signal_kind, day_end_micros)
                .await?
        }
    };

    repo.upsert_storage_snapshot(workspace_id, signal_kind, day, compressed_bytes, file_count)
        .await
}

/// Microseconds at the exclusive end of `day` (i.e. start of the next day).
fn day_end_micros(day: NaiveDate) -> i64 {
    (day + chrono::Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        .timestamp_micros()
}
