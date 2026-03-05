//! Simple authentication tests that don't require database

use api::auth::jwt::JwtAuth;
use uuid::Uuid;

#[test]
fn test_jwt_secret_and_expiry() {
    let _jwt_auth = JwtAuth::new("test-secret-key".to_string(), 24);

    // Just verify construction works
    // Full integration tests would require User struct creation
}

#[test]
fn test_bcrypt_password_hashing() {
    use bcrypt::{DEFAULT_COST, hash, verify};

    let password = "secure-password-123!@#";

    // Hash password
    let hash_result = hash(password, DEFAULT_COST).expect("Failed to hash password");

    assert_ne!(hash_result, password, "Hash should not equal plaintext");
    assert!(hash_result.len() > 50, "Bcrypt hash should be substantial");

    // Verify correct password
    let verify_result = verify(password, &hash_result).expect("Failed to verify password");
    assert!(verify_result, "Correct password should verify");

    // Verify incorrect password
    let wrong_verify = verify("wrong-password", &hash_result).expect("Failed to verify password");
    assert!(!wrong_verify, "Incorrect password should not verify");
}

#[test]
fn test_bcrypt_different_hashes() {
    use bcrypt::{DEFAULT_COST, hash};

    let password = "same-password";

    // Hash the same password twice
    let hash1 = hash(password, DEFAULT_COST).expect("Failed to hash");
    let hash2 = hash(password, DEFAULT_COST).expect("Failed to hash");

    // Hashes should be different due to salt
    assert_ne!(hash1, hash2, "Hashes should differ due to random salt");
}

#[test]
fn test_uuid_generation() {
    // Test UUID generation for IDs
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    assert_ne!(id1, id2, "UUIDs should be unique");

    // Test UUID string conversion
    let id_str = id1.to_string();
    let parsed_id = Uuid::parse_str(&id_str).expect("Failed to parse UUID");

    assert_eq!(id1, parsed_id, "UUID should round-trip through string");
}

#[test]
fn test_api_key_format() {
    // Test that we can generate random keys
    use rand::Rng;
    use rand::distributions::Alphanumeric;

    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();

    assert_eq!(key.len(), 32);
    assert!(key.chars().all(|c| c.is_alphanumeric()));
}
