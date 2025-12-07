//! Test helper modules

pub mod api_client;
pub mod fixtures;
pub mod grpc_client;
pub mod test_helpers;

// Database client kept for setup/cleanup only (not for test assertions)
#[allow(dead_code)]
pub(crate) mod db_client;

// Re-export commonly used items for black-box testing
pub use api_client::ApiClient;
pub use fixtures::*;
pub use grpc_client::OtlpClient;
pub use test_helpers::*;
