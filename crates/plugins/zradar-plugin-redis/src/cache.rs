//! Redis cache implementation

use redis::AsyncCommands;
use redis::aio::ConnectionManager;

/// Redis cache client
pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    /// Create new Redis cache
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;

        Ok(Self { conn })
    }

    /// Get value
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let value: Option<Vec<u8>> = conn.get(key).await?;
        Ok(value)
    }

    /// Set value with optional TTL
    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.clone();

        if let Some(ttl) = ttl_seconds {
            let _: () = conn.set_ex(key, value, ttl).await?;
        } else {
            let _: () = conn.set(key, value).await?;
        }

        Ok(())
    }

    /// Delete value
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let mut conn = self.conn.clone();
        let _: () = conn.del(key).await?;
        Ok(())
    }

    /// Check if key exists
    pub async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let mut conn = self.conn.clone();
        let exists: bool = conn.exists(key).await?;
        Ok(exists)
    }

    /// Health check
    pub async fn health_check(&self) -> anyhow::Result<bool> {
        let mut conn = self.conn.clone();
        let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(pong == "PONG")
    }
}
