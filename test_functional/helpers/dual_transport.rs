//! Macro helpers for running the same scenario over HTTP and gRPC.

/// Run `$body` twice: once over HTTP query/admin, once over gRPC.
///
/// Expands to `{name}__http` and `{name}__grpc` so nextest can run both in
/// parallel with the rest of the suite.
#[macro_export]
macro_rules! dual_transport_test {
    ($name:ident, $body:ident) => {
        ::paste::paste! {
            #[tokio::test]
            #[ignore]
            async fn [<$name __http>]() -> Result<()> {
                let mut env = TestEnv::setup_with_transport(Transport::Http).await?;
                $body(env)
                    .await
                    .with_context(|| concat!(stringify!($name), " [http]"))
            }

            #[tokio::test]
            #[ignore]
            async fn [<$name __grpc>]() -> Result<()> {
                let mut env = TestEnv::setup_with_transport(Transport::Grpc).await?;
                $body(env)
                    .await
                    .with_context(|| concat!(stringify!($name), " [grpc]"))
            }
        }
    };
}
