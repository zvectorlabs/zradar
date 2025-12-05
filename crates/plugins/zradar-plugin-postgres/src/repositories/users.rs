//! PostgreSQL user repository implementation

use async_trait::async_trait;
use sqlx::FromRow;
use std::sync::Arc;
use uuid::Uuid;

use zradar_traits::{UserRepository, User, UpdateUserRequest};
use crate::client::PostgresClient;

/// PostgreSQL row type for users
#[derive(Debug, Clone, FromRow)]
struct UserRow {
    id: Uuid,
    email: String,
    password_hash: String,
    full_name: Option<String>,
    is_active: bool,
    email_verified: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_login_at: Option<chrono::DateTime<chrono::Utc>>,
    metadata: serde_json::Value,
}

impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        User {
            id: row.id,
            email: row.email,
            password_hash: row.password_hash,
            full_name: row.full_name,
            is_active: row.is_active,
            email_verified: row.email_verified,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_login_at: row.last_login_at,
            metadata: row.metadata,
        }
    }
}

/// PostgreSQL implementation of UserRepository
pub struct PostgresUserRepository {
    client: Arc<PostgresClient>,
}

impl PostgresUserRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl UserRepository for PostgresUserRepository {
    async fn create_user(
        &self,
        email: String,
        password_hash: String,
        full_name: Option<String>,
    ) -> anyhow::Result<User> {
        let row = sqlx::query_as::<_, UserRow>(
            r#"
            INSERT INTO users (email, password_hash, full_name)
            VALUES ($1, $2, $3)
            RETURNING *
            "#,
        )
        .bind(&email)
        .bind(&password_hash)
        .bind(&full_name)
        .fetch_one(self.client.pool())
        .await?;
        
        Ok(row.into())
    }
    
    async fn get_user(&self, id: Uuid) -> anyhow::Result<Option<User>> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT * FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.client.pool())
        .await?;
        
        Ok(row.map(Into::into))
    }
    
    async fn get_user_by_email(&self, email: &str) -> anyhow::Result<Option<User>> {
        let row = sqlx::query_as::<_, UserRow>(
            "SELECT * FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(self.client.pool())
        .await?;
        
        Ok(row.map(Into::into))
    }
    
    async fn update_user(&self, id: Uuid, updates: UpdateUserRequest) -> anyhow::Result<User> {
        let row = sqlx::query_as::<_, UserRow>(
            r#"
            UPDATE users
            SET full_name = COALESCE($2, full_name),
                metadata = COALESCE($3, metadata),
                updated_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(updates.full_name)
        .bind(updates.metadata)
        .fetch_one(self.client.pool())
        .await?;
        
        Ok(row.into())
    }
    
    async fn update_last_login(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE users SET last_login_at = NOW() WHERE id = $1",
        )
        .bind(id)
        .execute(self.client.pool())
        .await?;
        
        Ok(())
    }
}

