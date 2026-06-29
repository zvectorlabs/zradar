//! gRPC transport layer for zradar Query and Admin APIs.
//!
//! This module provides tonic-based gRPC service implementations that delegate
//! to the shared service traits defined in `zradar-traits/src/services/`.

/// Generated protobuf types and service stubs for the Query API.
pub mod query_proto {
    tonic::include_proto!("zradar.query.v1");

    /// File descriptor set for server reflection.
    pub const QUERY_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("zradar_query_v1_descriptor");
}

/// Generated protobuf types and service stubs for the Admin API.
pub mod admin_proto {
    tonic::include_proto!("zradar.admin.v1");

    /// File descriptor set for server reflection.
    pub const ADMIN_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("zradar_admin_v1_descriptor");
}

pub mod auth;
pub mod conversions;
pub mod errors;

// ── Query API handlers ──────────────────────────────────────────────
pub mod analytics_handler;
pub mod query_handler;

// ── Admin API handlers ──────────────────────────────────────────────
pub mod audit_handler;
pub mod policy_handler;
pub mod retention_handler;
pub mod settings_handler;
