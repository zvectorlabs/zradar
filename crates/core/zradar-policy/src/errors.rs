use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("policy not found")]
    NotFound,
    #[error("invalid policy: {0}")]
    Invalid(String),
    #[error("policy store unavailable: {0}")]
    StoreUnavailable(String),
    #[error("usage reader unavailable: {0}")]
    UsageUnavailable(String),
    #[error("threshold sink failed: {0}")]
    ThresholdSinkFailed(String),
    #[error("decision audit sink failed: {0}")]
    DecisionAuditSinkFailed(String),
}
