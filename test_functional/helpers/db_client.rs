//! Database client for verification queries

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// Database client for test verification
pub struct DbClient {
    pub pg_pool: PgPool,
}

impl DbClient {
    /// Create a new database client
    pub async fn new(database_url: &str) -> Result<Self> {
        let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        Ok(Self { pg_pool })
    }

    // ========================================================================
    // PostgreSQL - Users
    // ========================================================================

    /// Count total users
    pub async fn count_users(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    /// Get user by email
    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<User>> {
        let user = sqlx::query_as::<_, User>(
            "SELECT id, email, display_name, is_active, is_system_admin, created_at 
             FROM users WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pg_pool)
        .await?;

        Ok(user)
    }

    /// Check if user exists
    pub async fn user_exists(&self, email: &str) -> Result<bool> {
        let row: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)")
            .bind(email)
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    // ========================================================================
    // PostgreSQL - Organizations
    // ========================================================================

    /// Count total organizations
    pub async fn count_organizations(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM organizations")
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    /// Get organization by ID
    pub async fn get_organization(&self, org_id: &Uuid) -> Result<Option<Organization>> {
        let org = sqlx::query_as::<_, Organization>(
            "SELECT id, name, display_name, created_at, updated_at 
             FROM organizations WHERE id = $1",
        )
        .bind(org_id)
        .fetch_optional(&self.pg_pool)
        .await?;

        Ok(org)
    }

    /// Organization exists
    pub async fn organization_exists(&self, name: &str) -> Result<bool> {
        let row: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM organizations WHERE name = $1)")
                .bind(name)
                .fetch_one(&self.pg_pool)
                .await?;
        Ok(row.0)
    }

    // ========================================================================
    // PostgreSQL - Projects
    // ========================================================================

    /// Count total projects
    pub async fn count_projects(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM projects")
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    /// Count projects for an organization
    pub async fn count_projects_for_org(&self, org_id: &Uuid) -> Result<i64> {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM projects WHERE organization_id = $1")
                .bind(org_id)
                .fetch_one(&self.pg_pool)
                .await?;
        Ok(row.0)
    }

    /// Get project by ID
    pub async fn get_project(&self, project_id: &Uuid) -> Result<Option<Project>> {
        let project = sqlx::query_as::<_, Project>(
            "SELECT id, organization_id, name, display_name, created_at, updated_at 
             FROM projects WHERE id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pg_pool)
        .await?;

        Ok(project)
    }

    /// Project exists
    pub async fn project_exists(&self, name: &str) -> Result<bool> {
        let row: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM projects WHERE name = $1)")
            .bind(name)
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    // ========================================================================
    // PostgreSQL - API Keys
    // ========================================================================

    /// Count total API keys
    pub async fn count_api_keys(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM api_keys")
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    /// Count API keys for a project
    pub async fn count_api_keys_for_project(&self, project_id: &Uuid) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM api_keys WHERE project_id = $1")
            .bind(project_id)
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    /// Get API key by ID
    pub async fn get_api_key(&self, key_id: &Uuid) -> Result<Option<ApiKey>> {
        let key = sqlx::query_as::<_, ApiKey>(
            "SELECT id, project_id, name, key_hash, is_revoked, created_at, revoked_at 
             FROM api_keys WHERE id = $1",
        )
        .bind(key_id)
        .fetch_optional(&self.pg_pool)
        .await?;

        Ok(key)
    }

    /// Check if API key is revoked
    pub async fn is_api_key_revoked(&self, key_id: &Uuid) -> Result<bool> {
        let row: (bool,) = sqlx::query_as("SELECT is_revoked FROM api_keys WHERE id = $1")
            .bind(key_id)
            .fetch_one(&self.pg_pool)
            .await?;
        Ok(row.0)
    }

    // ========================================================================
    // Cleanup Methods
    // ========================================================================

    /// Clean up test data by pattern
    pub async fn cleanup_test_data(&self, test_id: &str) -> Result<()> {
        let pattern = format!("%{}%", test_id);

        // Delete in reverse dependency order
        sqlx::query("DELETE FROM api_keys WHERE name LIKE $1")
            .bind(&pattern)
            .execute(&self.pg_pool)
            .await?;

        sqlx::query("DELETE FROM projects WHERE name LIKE $1")
            .bind(&pattern)
            .execute(&self.pg_pool)
            .await?;

        sqlx::query("DELETE FROM organizations WHERE name LIKE $1")
            .bind(&pattern)
            .execute(&self.pg_pool)
            .await?;

        sqlx::query("DELETE FROM users WHERE email LIKE $1")
            .bind(&pattern)
            .execute(&self.pg_pool)
            .await?;

        Ok(())
    }

    /// Truncate all test tables (use with caution!)
    pub async fn truncate_all(&self) -> Result<()> {
        sqlx::query("TRUNCATE TABLE api_keys CASCADE")
            .execute(&self.pg_pool)
            .await?;

        sqlx::query("TRUNCATE TABLE projects CASCADE")
            .execute(&self.pg_pool)
            .await?;

        sqlx::query("TRUNCATE TABLE organizations CASCADE")
            .execute(&self.pg_pool)
            .await?;

        // Don't truncate users (keep admin user)

        Ok(())
    }
}

// ============================================================================
// Database Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub is_active: bool,
    pub is_system_admin: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Organization {
    pub id: Uuid,
    pub name: String,
    pub display_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Project {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub display_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ApiKey {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub key_hash: String,
    pub is_revoked: bool,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}
