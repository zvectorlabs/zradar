//! S3 block storage implementation

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use zradar_traits::BlockStorage;

/// S3 block storage
pub struct S3BlockStorage {
    client: Client,
    bucket: String,
}

impl S3BlockStorage {
    /// Create new S3 storage
    pub async fn new(bucket: String, region: String) -> anyhow::Result<Self> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region))
            .load()
            .await;

        let client = Client::new(&config);

        Ok(Self { client, bucket })
    }

    /// Create from configuration
    pub async fn from_config(config: &serde_json::Value) -> anyhow::Result<Self> {
        let bucket = config["bucket"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("S3 bucket required"))?
            .to_string();

        let region = config["region"].as_str().unwrap_or("us-east-1").to_string();

        Self::new(bucket, region).await
    }
}

#[async_trait]
impl BlockStorage for S3BlockStorage {
    async fn upload(&self, key: &str, data: &[u8]) -> anyhow::Result<String> {
        let body = ByteStream::from(data.to_vec());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;

        Ok(format!("s3://{}/{}", self.bucket, key))
    }

    async fn download(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let data = resp.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn cleanup(&self, key: &str) -> anyhow::Result<()> {
        // S3 uses lifecycle policies - just log
        tracing::debug!(key = %key, "S3 cleanup (relies on lifecycle policy)");
        Ok(())
    }
}
