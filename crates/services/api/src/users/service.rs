//! Authentication service - business logic

use std::sync::Arc;

use super::types::*;
use crate::audit::{AuditEvent, AuditLogger, AuditStatus};
use crate::auth::TokenAuth;
use crate::errors::{ControlError, Result};

/// Authentication service for user management
pub struct AuthService {
    pub user_storage: Arc<dyn UserRepository>,
    pub jwt_auth: Arc<dyn TokenAuth>,
    pub audit: Arc<dyn AuditLogger>,
}

impl AuthService {
    /// Create a new AuthService
    pub fn new(
        user_storage: Arc<dyn UserRepository>,
        jwt_auth: Arc<dyn TokenAuth>,
        audit: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            user_storage,
            jwt_auth,
            audit,
        }
    }

    /// Register a new user
    pub async fn register(&self, req: RegisterRequest) -> Result<AuthResponse> {
        // Validate email format
        if !req.email.contains('@') {
            return Err(ControlError::InvalidInput(
                "Invalid email format".to_string(),
            ));
        }

        // Check if user already exists
        if self
            .user_storage
            .get_user_by_email(&req.email)
            .await?
            .is_some()
        {
            return Err(ControlError::Conflict(
                "Email already registered".to_string(),
            ));
        }

        // Hash password
        let password_hash = bcrypt::hash(&req.password, bcrypt::DEFAULT_COST)
            .map_err(|_| ControlError::PasswordHash)?;

        // Create user
        let user = self
            .user_storage
            .create_user(req.email.clone(), password_hash, req.full_name)
            .await?;

        // Generate JWT token
        let token = self.jwt_auth.generate_token(&user)?;

        // Log registration
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: None,
                user_id: Some(user.id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user.id),
                actor_ip: None,
                action: "user.registered".to_string(),
                resource_type: Some("user".to_string()),
                resource_id: Some(user.id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::info!(user_id = %user.id, email = %user.email, "User registered");

        Ok(AuthResponse {
            token,
            user: user.into(),
        })
    }

    /// Login with email and password
    pub async fn login(&self, req: LoginRequest) -> Result<AuthResponse> {
        // Get user by email
        let user = match self.user_storage.get_user_by_email(&req.email).await? {
            Some(u) => u,
            None => {
                let _ = self
                    .audit
                    .log(AuditEvent {
                        organization_id: None,
                        user_id: None,
                        actor_type: Some("user".to_string()),
                        actor_id: None,
                        actor_ip: None,
                        action: "user.login_failed".to_string(),
                        resource_type: Some("user".to_string()),
                        resource_id: None,
                        status: AuditStatus::Failure,
                        details: Some(
                            serde_json::json!({"reason": "user_not_found", "email": req.email}),
                        ),
                    })
                    .await;
                return Err(ControlError::AuthenticationFailed(
                    "Invalid email or password".to_string(),
                ));
            }
        };

        // Verify password
        let valid = bcrypt::verify(&req.password, &user.password_hash)
            .map_err(|_| ControlError::PasswordHash)?;

        if !valid {
            let _ = self
                .audit
                .log(AuditEvent {
                    organization_id: None,
                    user_id: Some(user.id),
                    actor_type: Some("user".to_string()),
                    actor_id: Some(user.id),
                    actor_ip: None,
                    action: "user.login_failed".to_string(),
                    resource_type: Some("user".to_string()),
                    resource_id: Some(user.id),
                    status: AuditStatus::Failure,
                    details: Some(serde_json::json!({"reason": "invalid_password"})),
                })
                .await;

            return Err(ControlError::AuthenticationFailed(
                "Invalid email or password".to_string(),
            ));
        }

        // Check if user is active
        if !user.is_active {
            return Err(ControlError::AuthenticationFailed(
                "Account is inactive".to_string(),
            ));
        }

        // Update last login
        let _ = self.user_storage.update_last_login(user.id).await;

        // Generate JWT token
        let token = self.jwt_auth.generate_token(&user)?;

        // Log successful login
        let _ = self
            .audit
            .log(AuditEvent {
                organization_id: None,
                user_id: Some(user.id),
                actor_type: Some("user".to_string()),
                actor_id: Some(user.id),
                actor_ip: None,
                action: "user.login".to_string(),
                resource_type: Some("user".to_string()),
                resource_id: Some(user.id),
                status: AuditStatus::Success,
                details: None,
            })
            .await;

        tracing::info!(user_id = %user.id, email = %user.email, "User logged in");

        Ok(AuthResponse {
            token,
            user: user.into(),
        })
    }

    /// Get current user info
    pub fn get_current_user(&self, user: User) -> UserResponse {
        user.into()
    }
}
