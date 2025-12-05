//! Authentication implementations
//!
//! This module contains JWT and API key authentication adapters.

pub mod jwt;
pub mod token_auth;
pub mod api_key;
pub mod api_key_validator;

// Re-export main types
pub use jwt::{JwtAuth, Claims};
pub use token_auth::TokenAuth;
pub use api_key::{ApiKeyAuth, RequestContext, CachedKeyInfo};
pub use api_key_validator::ApiKeyValidator;

// Re-export KeyGenerator from api_keys module for convenience
pub use crate::api_keys::service::KeyGenerator;

/// Default key generator implementation
pub struct DefaultKeyGenerator;

impl KeyGenerator for DefaultKeyGenerator {
    fn generate_key(prefix: &str) -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..32).map(|_| rng.sample(rand::distributions::Standard)).collect();
        let random_str = hex::encode(random_bytes);
        format!("{}_{}", prefix, random_str)
    }

    fn hash_key(key: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }
}
