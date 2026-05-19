use async_trait::async_trait;
use uuid::Uuid;
use zradar_models::{NewRetentionPolicy, RetentionPolicy};

#[async_trait]
pub trait RetentionPolicyRepository: Send + Sync {
    async fn get_policy(&self, org_id: Uuid) -> anyhow::Result<Option<RetentionPolicy>>;
    async fn list_policies(&self) -> anyhow::Result<Vec<RetentionPolicy>>;
    async fn upsert_policy(&self, policy: NewRetentionPolicy) -> anyhow::Result<RetentionPolicy>;
}
