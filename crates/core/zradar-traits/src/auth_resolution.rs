//! Transport-agnostic authorization result shared by query and admin authorizers.

use zradar_models::RequestContext;

/// Resolved identity and capability keys from an authorizer.
///
/// Wire capability strings (`read_traces`, `admin`, …) are parsed into
/// [`Capability`](crate::Capability) by the transport layer before handlers run.
#[derive(Debug, Clone)]
pub struct AuthResolution {
    /// Resolved workspace context.
    pub context: RequestContext,
    /// Zero or more zradar capability identifiers.
    /// Empty means standalone mode — all handler capability checks pass.
    pub capability_keys: Vec<String>,
}
