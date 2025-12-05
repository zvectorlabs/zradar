//! User types and DTOs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

// Re-export from zradar_traits
pub use zradar_traits::{User, UpdateUserRequest, UserRepository};

// ============================================================================
// HTTP-specific types (not in traits)
// ============================================================================

/// Request to register a new user
#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterRequest {
    #[schema(example = "user@example.com")]
    pub email: String,
    #[schema(example = "securepassword123")]
    pub password: String,
    #[schema(example = "John Doe")]
    pub full_name: Option<String>,
}

/// Request to login
#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    #[schema(example = "user@example.com")]
    pub email: String,
    #[schema(example = "securepassword123")]
    pub password: String,
}

/// Login/Register response with JWT token
#[derive(Debug, Serialize, ToSchema)]
pub struct AuthResponse {
    pub token: String,
    pub user: UserResponse,
}

/// User response (without sensitive data)
#[derive(Debug, Serialize, ToSchema)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub full_name: Option<String>,
    pub is_active: bool,
    pub email_verified: bool,
    pub created_at: DateTime<Utc>,
    pub last_login_at: Option<DateTime<Utc>>,
}

impl From<User> for UserResponse {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            email: user.email,
            full_name: user.full_name,
            is_active: user.is_active,
            email_verified: user.email_verified,
            created_at: user.created_at,
            last_login_at: user.last_login_at,
        }
    }
}

/// Refresh token response
#[derive(Debug, Serialize, ToSchema)]
pub struct RefreshResponse {
    pub token: String,
}

