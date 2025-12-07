//! Authentication implementations
//!
//! This module contains JWT and API key authentication adapters.

pub mod api_key;
pub mod api_key_validator;
pub mod jwt;
pub mod token_auth;

// Re-export main types
pub use api_key::{ApiKeyAuth, CachedKeyInfo, RequestContext};
pub use api_key_validator::ApiKeyValidator;
pub use jwt::{Claims, JwtAuth};
pub use token_auth::TokenAuth;

// Re-export KeyGenerator from api_keys module for convenience
pub use crate::api_keys::service::KeyGenerator;

/// Default key generator implementation
pub struct DefaultKeyGenerator;

impl KeyGenerator for DefaultKeyGenerator {
    fn generate_key(prefix: &str) -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..32)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect();
        let random_str = hex::encode(random_bytes);
        format!("{}_{}", prefix, random_str)
    }

    fn hash_key(key: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }
}
