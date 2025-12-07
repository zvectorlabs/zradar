//! Functional tests for zradar
//!
//! These are black-box API tests that verify zradar behavior through
//! public HTTP and gRPC endpoints only. No direct database access.
//!
//! Run with: cargo test --test functional_tests -- --ignored
//!
//! Or use the test runner: ./scripts/test-rust-functional.sh

// Import the library
use functional_tests::*;

// Include all test scenario modules
// Each module contains #[test] functions that will be discovered by cargo test
#[path = "scenarios/test_health.rs"]
mod test_health;

#[path = "scenarios/test_auth.rs"]
mod test_auth;

#[path = "scenarios/test_organizations.rs"]
mod test_organizations;

#[path = "scenarios/test_projects.rs"]
mod test_projects;

#[path = "scenarios/test_api_keys.rs"]
mod test_api_keys;

#[path = "scenarios/test_tracing.rs"]
mod test_tracing;

#[path = "scenarios/test_e2e.rs"]
mod test_e2e;

#[path = "scenarios/test_scores.rs"]
mod test_scores;

#[path = "scenarios/test_query_api.rs"]
mod test_query_api;

#[path = "scenarios/test_telemetry_storage.rs"]
mod test_telemetry_storage;

#[path = "scenarios/test_span_types.rs"]
mod test_span_types;

#[path = "scenarios/test_analytics.rs"]
mod test_analytics;
