//! Plugin error types

use thiserror::Error;

/// Plugin-related errors
#[derive(Error, Debug)]
pub enum PluginError {
    #[error("Plugin not found: {0}")]
    NotFound(String),

    #[error("Plugin already registered: {0}")]
    AlreadyRegistered(String),

    #[error("Invalid plugin configuration: {0}")]
    InvalidConfig(String),

    #[error("Plugin initialization failed: {0}")]
    InitializationFailed(String),

    #[error("Plugin load failed: {0}")]
    LoadFailed(String),

    #[error("Plugin version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: String, actual: String },

    #[error("Plugin type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Plugin dependency missing: {0}")]
    DependencyMissing(String),

    #[error("Plugin operation failed: {0}")]
    OperationFailed(String),

    #[error("Migration failed: {0}")]
    MigrationFailed(String),

    #[error("Migration checksum mismatch: {0}")]
    MigrationChecksumMismatch(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, PluginError>;
