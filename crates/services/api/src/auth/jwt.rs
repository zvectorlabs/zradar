//! JWT authentication

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{ControlError, Result};
use crate::domain::users::User;
use crate::auth::token_auth::TokenAuth;

/// JWT authentication service
pub struct JwtAuth {
    secret: String,
    expiry_hours: i64,
}

/// JWT claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,  // user_id
    pub email: String,
    pub exp: usize,
    pub iat: usize,
}

impl JwtAuth {
    pub fn new(secret: String, expiry_hours: u32) -> Self {
        Self {
            secret,
            expiry_hours: expiry_hours as i64,
        }
    }

    pub fn generate_token(&self, user: &User) -> Result<String> {
        let now = Utc::now();
        let expiration = now + Duration::hours(self.expiry_hours);

        let claims = Claims {
            sub: user.id,
            email: user.email.clone(),
            iat: now.timestamp() as usize,
            exp: expiration.timestamp() as usize,
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )?;

        tracing::info!(user_id = %user.id, email = %user.email, "JWT token generated");

        Ok(token)
    }

    pub fn validate_token(&self, token: &str) -> Result<Claims> {
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|e| {
            tracing::warn!("JWT validation failed: {}", e);
            ControlError::Jwt(e)
        })?;

        Ok(token_data.claims)
    }
}

impl TokenAuth for JwtAuth {
    fn generate_token(&self, user: &User) -> Result<String> {
        self.generate_token(user)
    }
    
    fn validate_token(&self, token: &str) -> Result<Claims> {
        self.validate_token(token)
    }
}

