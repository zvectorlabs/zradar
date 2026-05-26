//! zradar-native authorization capabilities.
//!
//! HTTP handlers depend on these capabilities instead of transport headers or
//! gateway-specific permission strings. The `api` crate re-exports `Capability`
//! from here for handler convenience.

/// zradar-native authorization capabilities.
///
/// When the `AdminAuthorizer` returns `capability_keys` from a gateway request,
/// those wire strings are converted into this enum before reaching handlers.
/// An empty capabilities list means all checks pass (standalone API-key mode).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    ReadTraces,
    ReadDashboards,
    ReadLogs,
    ReadMetrics,
    ReadSettings,
    WriteSettings,
    Admin,
}

impl Capability {
    /// Convert a wire capability key string into a `Capability` variant.
    ///
    /// Returns `None` for unknown keys — forward-compatible with new scopes.
    pub fn from_key(key: &str) -> Option<Self> {
        match key {
            "read_traces" => Some(Self::ReadTraces),
            "read_dashboards" => Some(Self::ReadDashboards),
            "read_logs" => Some(Self::ReadLogs),
            "read_metrics" => Some(Self::ReadMetrics),
            "read_settings" => Some(Self::ReadSettings),
            "write_settings" => Some(Self::WriteSettings),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}
