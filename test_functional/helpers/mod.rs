//! Test helper modules

pub mod api_client;
pub mod dual_transport;
pub mod fixtures;
pub mod grpc_client;
pub mod nim_mocks;
pub mod polling;
pub mod query_transport;
pub mod test_env;
pub mod test_helpers;
pub mod transport;
pub mod transport_api;
pub mod zradar_grpc_client;

#[allow(dead_code)]
pub mod db_client;

// Re-export commonly used items for black-box testing
pub use api_client::ApiClient;
pub use db_client::DbClient;
pub use fixtures::*;
pub use grpc_client::{OtlpClient, SpanDefExt};
pub use polling::{
    DEFAULT_POLL_INTERVAL, DEFAULT_POLL_TIMEOUT, poll_until, wait_for_items,
    wait_for_items_default, wait_for_trace, wait_for_trace_default,
};
pub use query_transport::{
    ErrorBreakdownView, QueryTransportClient, SpanFilters, SpanView, TraceView,
};
pub use test_env::{TestEnv, TestSession};
pub use test_helpers::*;
pub use transport::Transport;
pub use transport_api::TransportApiClient;
pub use zradar_grpc_client::{
    SpanQueryParams, WorkspaceSettingsInput, ZradarAdminClient, ZradarGrpcClients,
    ZradarQueryClient, grpc_not_ready, recent_time_range, timestamp_hours_ago, timestamp_now,
};
