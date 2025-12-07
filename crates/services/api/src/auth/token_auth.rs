//! Token authentication trait

use crate::auth::jwt::Claims;
use crate::domain::users::User;
use crate::errors::Result;

/// Trait for token-based authentication
pub trait TokenAuth: Send + Sync {
    /// Generate a token for a user
    fn generate_token(&self, user: &User) -> Result<String>;

    /// Validate a token and return claims
    fn validate_token(&self, token: &str) -> Result<Claims>;
}
