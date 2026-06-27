//! Test helper modules

pub mod api_client;
pub mod fixtures;
pub mod grpc_client;
pub mod nim_mocks;
pub mod polling;
pub mod test_env;
pub mod test_helpers;

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
pub use test_env::{TestEnv, TestSession};
pub use test_helpers::*;
