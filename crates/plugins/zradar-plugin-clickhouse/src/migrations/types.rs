//! Types for migration system

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: String,
    pub description: String,
    pub filepath: PathBuf,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: String,
    pub description: String,
    pub applied_at: String,
    pub checksum: String,
    pub execution_time_ms: u32,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Migration file not found: {0}")]
    FileNotFound(String),
    
    #[error("Invalid migration filename format: {0}")]
    InvalidFilename(String),
    
    #[error("Migration checksum mismatch for version {version}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        version: String,
        expected: String,
        actual: String,
    },
    
    #[error("Migration failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Database error: {0}")]
    DatabaseError(#[from] clickhouse::error::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type MigrationResult<T> = Result<T, MigrationError>;

