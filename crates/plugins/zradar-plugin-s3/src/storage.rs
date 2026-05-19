//! S3 block storage implementation using opendal

use async_trait::async_trait;
use opendal::Operator;
use opendal::services::S3;
use zradar_traits::BlockStorage;

/// S3 block storage backed by opendal
pub struct S3BlockStorage {
    operator: Operator,
    bucket: String,
}

impl S3BlockStorage {
    /// Create new S3 storage
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut builder = S3::default().bucket(&bucket).region(&region);

        if let Some(endpoint_url) = endpoint {
            builder = builder.endpoint(&endpoint_url);
        }

        let operator = Operator::new(builder)?.finish();

        Ok(Self { operator, bucket })
    }

    /// Create from configuration
    pub async fn from_config(config: &serde_json::Value) -> anyhow::Result<Self> {
        let bucket = config["bucket"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("S3 bucket required"))?
            .to_string();

        let region = config["region"].as_str().unwrap_or("us-east-1").to_string();

        let endpoint = config["endpoint"].as_str().map(String::from);

        Self::new(bucket, region, endpoint).await
    }
}

#[async_trait]
impl BlockStorage for S3BlockStorage {
    async fn upload(&self, key: &str, data: &[u8]) -> anyhow::Result<String> {
        self.operator.write(key, data.to_vec()).await?;
        Ok(format!("s3://{}/{}", self.bucket, key))
    }

    async fn download(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let data = self.operator.read(key).await?;
        Ok(data.to_vec())
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.operator.delete(key).await?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.operator.is_exist(key).await?)
    }

    async fn cleanup(&self, key: &str) -> anyhow::Result<()> {
        tracing::debug!(key = %key, "S3 cleanup (relies on lifecycle policy)");
        Ok(())
    }
}
