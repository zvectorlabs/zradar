//! Query/admin transport selection for dual HTTP + gRPC functional tests.

/// Which query/admin API transport a functional test uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Http,
    Grpc,
}

impl Transport {
    pub fn label(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Grpc => "grpc",
        }
    }
}
