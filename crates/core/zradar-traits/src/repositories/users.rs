//! User repository trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// User entity
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct User {
    pub id: Uuid,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub full_name: Option<String>,
    pub is_active: bool,
    pub email_verified: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login_at: Option<DateTime<Utc>>,
    #[sqlx(json)]
    pub metadata: serde_json::Value,
}

/// User update request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct UpdateUserRequest {
    pub full_name: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// Repository trait for user persistence
#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create_user(&self, email: String, password_hash: String, full_name: Option<String>) -> anyhow::Result<User>;
    async fn get_user(&self, id: Uuid) -> anyhow::Result<Option<User>>;
    async fn get_user_by_email(&self, email: &str) -> anyhow::Result<Option<User>>;
    async fn update_user(&self, id: Uuid, updates: UpdateUserRequest) -> anyhow::Result<User>;
    async fn update_last_login(&self, id: Uuid) -> anyhow::Result<()>;
}
