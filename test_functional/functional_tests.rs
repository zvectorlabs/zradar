//! Functional tests for zradar
//!
//! Black-box API tests that verify zradar behavior through public HTTP and gRPC
//! endpoints only. No direct database access.
//!
//! Run with: cargo test --test functional_tests -- --ignored
//!
//! Or use the test runner: ./scripts/test-rust-functional.sh

pub use functional_tests::*;

// Core telemetry tests
#[path = "scenarios/test_health.rs"]
mod test_health;

#[path = "scenarios/test_tracing.rs"]
mod test_tracing;

#[path = "scenarios/test_e2e.rs"]
mod test_e2e;

#[path = "scenarios/test_query_api.rs"]
mod test_query_api;

#[path = "scenarios/test_telemetry_storage.rs"]
mod test_telemetry_storage;

#[path = "scenarios/test_parquet_metadata.rs"]
mod test_parquet_metadata;

#[path = "scenarios/test_span_types.rs"]
mod test_span_types;

#[path = "scenarios/test_analytics.rs"]
mod test_analytics;

#[path = "scenarios/test_audit_logging.rs"]
mod test_audit_logging;

#[path = "scenarios/test_metrics.rs"]
mod test_metrics;

#[path = "scenarios/test_logs.rs"]
mod test_logs;

#[path = "scenarios/test_retention.rs"]
mod test_retention;

#[path = "scenarios/test_policy_enforcement.rs"]
mod test_policy_enforcement;

#[path = "scenarios/test_agent_load.rs"]
mod test_agent_load;

#[path = "scenarios/test_agent_analytics.rs"]
mod test_agent_analytics;

#[path = "scenarios/test_nemo_phase1.rs"]
mod test_nemo_phase1;

#[path = "scenarios/test_nemo_phase2.rs"]
mod test_nemo_phase2;

#[path = "scenarios/test_nemo_phase4.rs"]
mod test_nemo_phase4;

#[path = "scenarios/test_e2e_fixtures.rs"]
mod test_e2e_fixtures;

#[path = "scenarios/test_e2e_multi_tenant.rs"]
mod test_e2e_multi_tenant;

#[path = "scenarios/test_nemo_gap_fixes.rs"]
mod test_nemo_gap_fixes;

#[path = "scenarios/test_nim_collector.rs"]
mod test_nim_collector;

#[path = "scenarios/test_security.rs"]
mod test_security;
