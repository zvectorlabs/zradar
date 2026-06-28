use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zradar_models::WorkspaceId;
use zradar_models::{NewWorkspaceSettings, WorkspaceSettings};

#[async_trait]
pub trait SettingsRepository: Send + Sync {
    async fn get_settings(
        &self,
        workspace_id: WorkspaceId,
    ) -> anyhow::Result<Option<WorkspaceSettings>>;
    async fn upsert_settings(
        &self,
        settings: NewWorkspaceSettings,
    ) -> anyhow::Result<WorkspaceSettings>;
    async fn list_all_settings(&self) -> anyhow::Result<Vec<WorkspaceSettings>>;
}

#[derive(Clone)]
struct CacheEntry {
    settings: Option<WorkspaceSettings>,
    expires_at: Instant,
}

pub struct CachedSettingsRepository {
    inner: Arc<dyn SettingsRepository>,
    cache: DashMap<WorkspaceId, CacheEntry>,
    ttl: Duration,
}

impl CachedSettingsRepository {
    pub fn new(inner: Arc<dyn SettingsRepository>, ttl: Duration) -> Self {
        Self {
            inner,
            cache: DashMap::new(),
            ttl,
        }
    }
}

#[async_trait]
impl SettingsRepository for CachedSettingsRepository {
    async fn get_settings(
        &self,
        workspace_id: WorkspaceId,
    ) -> anyhow::Result<Option<WorkspaceSettings>> {
        let now = Instant::now();
        if let Some(entry) = self.cache.get(&workspace_id)
            && entry.expires_at > now
        {
            return Ok(entry.settings.clone());
        }

        let settings = self.inner.get_settings(workspace_id).await?;
        self.cache.insert(
            workspace_id,
            CacheEntry {
                settings: settings.clone(),
                expires_at: now + self.ttl,
            },
        );
        Ok(settings)
    }

    async fn upsert_settings(
        &self,
        settings: NewWorkspaceSettings,
    ) -> anyhow::Result<WorkspaceSettings> {
        let workspace_id = settings.workspace_id;
        let saved = self.inner.upsert_settings(settings).await?;
        self.cache.insert(
            workspace_id,
            CacheEntry {
                settings: Some(saved.clone()),
                expires_at: Instant::now() + self.ttl,
            },
        );
        Ok(saved)
    }

    async fn list_all_settings(&self) -> anyhow::Result<Vec<WorkspaceSettings>> {
        self.inner.list_all_settings().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct MockSettingsRepo {
        call_count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl SettingsRepository for MockSettingsRepo {
        async fn get_settings(
            &self,
            workspace_id: WorkspaceId,
        ) -> anyhow::Result<Option<WorkspaceSettings>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(Some(WorkspaceSettings {
                id: 1,
                workspace_id,
                traces_retention_days: 90,
                metrics_retention_days: 30,
                logs_retention_days: 30,
                max_ingestion_rate: None,
                file_push_interval_secs: 300,
                blocked: false,
                capture_llm_content_enabled: true,
                updated_at: 0,
            }))
        }

        async fn upsert_settings(
            &self,
            settings: NewWorkspaceSettings,
        ) -> anyhow::Result<WorkspaceSettings> {
            Ok(WorkspaceSettings {
                id: 1,
                workspace_id: settings.workspace_id,
                traces_retention_days: settings.traces_retention_days,
                metrics_retention_days: settings.metrics_retention_days,
                logs_retention_days: settings.logs_retention_days,
                max_ingestion_rate: settings.max_ingestion_rate,
                file_push_interval_secs: settings.file_push_interval_secs,
                blocked: settings.blocked,
                capture_llm_content_enabled: settings.capture_llm_content_enabled,
                updated_at: 0,
            })
        }

        async fn list_all_settings(&self) -> anyhow::Result<Vec<WorkspaceSettings>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_cache_hits_and_misses() {
        let calls = Arc::new(AtomicU32::new(0));
        let mock = Arc::new(MockSettingsRepo {
            call_count: calls.clone(),
        });
        let cached = CachedSettingsRepository::new(mock, Duration::from_secs(2));

        let ws_id = WorkspaceId::new();

        // 1. First call: miss, queries inner repository
        let settings = cached.get_settings(ws_id).await.unwrap();
        assert!(settings.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // 2. Second call: hit, should NOT query inner repository
        let settings = cached.get_settings(ws_id).await.unwrap();
        assert!(settings.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // 3. Expire the cache entry (manually modify expire time to simulate time pass)
        if let Some(mut entry) = cached.cache.get_mut(&ws_id) {
            entry.expires_at = Instant::now() - Duration::from_secs(1);
        }

        // 4. Third call: expired, should query inner repository again
        let settings = cached.get_settings(ws_id).await.unwrap();
        assert!(settings.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cache_update_on_upsert() {
        let calls = Arc::new(AtomicU32::new(0));
        let mock = Arc::new(MockSettingsRepo {
            call_count: calls.clone(),
        });
        let cached = CachedSettingsRepository::new(mock, Duration::from_secs(2));

        let ws_id = WorkspaceId::new();

        // Upsert settings
        let new_settings = NewWorkspaceSettings {
            workspace_id: ws_id,
            traces_retention_days: 45,
            metrics_retention_days: 15,
            logs_retention_days: 15,
            max_ingestion_rate: None,
            file_push_interval_secs: 100,
            blocked: false,
            capture_llm_content_enabled: false,
        };
        let _ = cached.upsert_settings(new_settings).await.unwrap();

        // Query settings: should be a cache hit (directly retrieved from populated cache)
        let settings = cached.get_settings(ws_id).await.unwrap().unwrap();
        assert_eq!(settings.traces_retention_days, 45);
        assert_eq!(calls.load(Ordering::SeqCst), 0); // No mock DB call made
    }
}
