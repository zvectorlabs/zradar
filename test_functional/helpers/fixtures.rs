//! Test fixtures and data generators

use fake::faker::company::en::*;
use fake::faker::internet::en::*;
use fake::faker::name::en::*;
use fake::Fake;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Test data generator
pub struct TestDataGenerator;

impl TestDataGenerator {
    // ========================================================================
    // User Data
    // ========================================================================
    
    /// Generate a unique test email
    pub fn email() -> String {
        // Use UUID instead of timestamp to avoid collisions in parallel tests
        let unique_id = Self::test_id();
        format!("test-{}@example.com", unique_id)
    }
    
    /// Generate a random email
    pub fn random_email() -> String {
        SafeEmail().fake()
    }
    
    /// Generate a display name
    pub fn display_name() -> String {
        Name().fake()
    }
    
    /// Generate a secure password
    pub fn password() -> String {
        "SecureTestPass123!".to_string()
    }
    
    // ========================================================================
    // Organization Data
    // ========================================================================
    
    /// Generate a unique organization name
    pub fn org_name() -> String {
        // Use UUID instead of timestamp to avoid collisions in parallel tests
        let unique_id = Self::test_id();
        format!("test-org-{}", unique_id)
    }
    
    /// Generate a random organization display name
    pub fn org_display_name() -> String {
        CompanyName().fake()
    }
    
    // ========================================================================
    // Project Data
    // ========================================================================
    
    /// Generate a unique project name
    pub fn project_name() -> String {
        // Use UUID instead of timestamp to avoid collisions in parallel tests
        let unique_id = Self::test_id();
        format!("test-project-{}", unique_id)
    }
    
    /// Generate a random project display name
    pub fn project_display_name() -> String {
        format!("{} Project", CompanyName().fake::<String>())
    }
    
    // ========================================================================
    // API Key Data
    // ========================================================================
    
    /// Generate a unique API key name
    pub fn api_key_name() -> String {
        // Use UUID instead of timestamp to avoid collisions in parallel tests
        let unique_id = Self::test_id();
        format!("test-key-{}", unique_id)
    }
    
    /// Generate API key description
    pub fn api_key_description() -> String {
        format!("Test API key created at {}", chrono::Utc::now())
    }
    
    // ========================================================================
    // Trace Data
    // ========================================================================
    
    /// Generate a random service name
    pub fn service_name() -> String {
        format!("{}-service", CompanySuffix().fake::<String>().to_lowercase())
    }
    
    /// Generate a random span name
    pub fn span_name() -> String {
        let operations = ["GET /api/users",
            "POST /api/orders",
            "database.query",
            "cache.get",
            "external.api.call",
            "process.payment",
            "send.email",
            "render.template"];
        
        use rand::seq::SliceRandom;
        operations.choose(&mut rand::thread_rng()).unwrap().to_string()
    }
    
    /// Generate a random trace ID (16 bytes)
    pub fn trace_id() -> [u8; 16] {
        use rand::Rng;
        rand::thread_rng().r#gen()
    }
    
    /// Generate a random span ID (8 bytes)
    pub fn span_id() -> [u8; 8] {
        use rand::Rng;
        rand::thread_rng().r#gen()
    }
    
    // ========================================================================
    // Utilities
    // ========================================================================
    
    /// Get current timestamp as string
    pub fn timestamp() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }
    
    /// Generate a unique test ID
    pub fn test_id() -> String {
        Uuid::new_v4().to_string()[..8].to_string()
    }
    
    /// Generate random UUID
    pub fn uuid() -> Uuid {
        Uuid::new_v4()
    }
}

/// Fixture builder for complete test scenarios
pub struct FixtureBuilder {
    org_name: Option<String>,
    org_display_name: Option<String>,
    project_name: Option<String>,
    project_display_name: Option<String>,
    api_key_name: Option<String>,
    api_key_description: Option<String>,
}

impl Default for FixtureBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FixtureBuilder {
    pub fn new() -> Self {
        Self {
            org_name: None,
            org_display_name: None,
            project_name: None,
            project_display_name: None,
            api_key_name: None,
            api_key_description: None,
        }
    }
    
    pub fn org_name(mut self, name: impl Into<String>) -> Self {
        self.org_name = Some(name.into());
        self
    }
    
    pub fn org_display_name(mut self, name: impl Into<String>) -> Self {
        self.org_display_name = Some(name.into());
        self
    }
    
    pub fn project_name(mut self, name: impl Into<String>) -> Self {
        self.project_name = Some(name.into());
        self
    }
    
    pub fn project_display_name(mut self, name: impl Into<String>) -> Self {
        self.project_display_name = Some(name.into());
        self
    }
    
    pub fn api_key_name(mut self, name: impl Into<String>) -> Self {
        self.api_key_name = Some(name.into());
        self
    }
    
    pub fn api_key_description(mut self, desc: impl Into<String>) -> Self {
        self.api_key_description = Some(desc.into());
        self
    }
    
    pub fn build(self) -> TestFixture {
        TestFixture {
            org_name: self.org_name.unwrap_or_else(TestDataGenerator::org_name),
            org_display_name: self
                .org_display_name
                .unwrap_or_else(TestDataGenerator::org_display_name),
            project_name: self.project_name.unwrap_or_else(TestDataGenerator::project_name),
            project_display_name: self
                .project_display_name
                .unwrap_or_else(TestDataGenerator::project_display_name),
            api_key_name: self.api_key_name.unwrap_or_else(TestDataGenerator::api_key_name),
            api_key_description: self
                .api_key_description
                .unwrap_or_else(TestDataGenerator::api_key_description),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestFixture {
    pub org_name: String,           // This becomes the display name (name field in API)
    pub org_display_name: String,   // Deprecated - same as org_name
    pub project_name: String,       // This becomes the display name (name field in API)
    pub project_display_name: String, // Deprecated - same as project_name
    pub api_key_name: String,
    pub api_key_description: String,
}

impl TestFixture {
    /// Get org slug (auto-generated from org_name)
    pub fn org_slug(&self) -> String {
        self.org_name.to_lowercase().replace(" ", "-").replace("_", "-")
    }
    
    /// Get project slug (auto-generated from project_name)
    pub fn project_slug(&self) -> String {
        self.project_name.to_lowercase().replace(" ", "-").replace("_", "-")
    }
}

impl TestFixture {
    pub fn new() -> Self {
        FixtureBuilder::new().build()
    }
    
    pub fn builder() -> FixtureBuilder {
        FixtureBuilder::new()
    }
}

impl Default for TestFixture {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Predefined Test Constants
// ============================================================================

pub const TEST_ADMIN_EMAIL: &str = "admin@example.com";
pub const TEST_ADMIN_PASSWORD: &str = "changeme123";

pub const TEST_USER_EMAIL: &str = "testuser@example.com";
pub const TEST_USER_PASSWORD: &str = "TestPass123!";
pub const TEST_USER_DISPLAY_NAME: &str = "Test User";

// Sample trace IDs for testing (hex strings)
pub const SAMPLE_TRACE_ID_1: &str = "0123456789abcdef0123456789abcdef";
pub const SAMPLE_TRACE_ID_2: &str = "fedcba9876543210fedcba9876543210";
pub const SAMPLE_SPAN_ID_1: &str = "0123456789abcdef";
pub const SAMPLE_SPAN_ID_2: &str = "fedcba9876543210";

