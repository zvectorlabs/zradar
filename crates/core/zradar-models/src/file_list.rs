//! File list and stream statistics models
//!
//! These models track every Parquet file written to local disk or S3, and
//! maintain per-stream aggregate statistics for fast overview queries.

use crate::WorkspaceId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single Parquet file registered in the file list.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct FileListEntry {
    /// Auto-increment primary key.
    pub id: i64,
    /// Workspace identifier.
    pub workspace_id: WorkspaceId,
    /// Signal type: "traces", "metrics", or "logs".
    pub signal_type: String,
    /// Stream name (derived from service_name for traces).
    pub stream_name: String,
    /// Date partition path component: "YYYY/MM/DD/HH".
    pub date: String,
    /// Full local path or S3 key.
    pub file_path: String,
    /// Storage location: "local" or "s3".
    pub location: String,
    /// Minimum timestamp in the file (microseconds since epoch).
    pub min_ts: i64,
    /// Maximum timestamp in the file (microseconds since epoch).
    pub max_ts: i64,
    /// Number of records (rows) in the file.
    pub records: i64,
    /// Estimated uncompressed size in bytes.
    pub original_size: i64,
    /// Actual Parquet file size on disk (compressed).
    pub compressed_size: i64,
    /// Whether this file has been marked for deletion.
    pub deleted: bool,
    /// File registration time (microseconds since epoch).
    pub created_at: i64,
    /// Last update time (microseconds since epoch).
    pub updated_at: i64,
}

/// Input for registering a new Parquet file (id is auto-generated).
#[derive(Debug, Clone)]
pub struct NewFileListEntry {
    pub workspace_id: WorkspaceId,
    pub signal_type: String,
    pub stream_name: String,
    /// Date partition: "YYYY/MM/DD/HH"
    pub date: String,
    pub file_path: String,
    pub location: String,
    /// Minimum span timestamp (microseconds since epoch).
    pub min_ts: i64,
    /// Maximum span end-time (microseconds since epoch).
    pub max_ts: i64,
    pub records: i64,
    pub original_size: i64,
    pub compressed_size: i64,
    pub created_at: i64,
    pub updated_at: i64,
    /// WAL offset that was flushed to produce this file (`None` for non-WAL writes).
    pub wal_replay_offset: Option<i64>,
}

/// Per-stream aggregated statistics.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct StreamStats {
    pub id: i64,
    pub workspace_id: WorkspaceId,
    pub signal_type: String,
    pub stream_name: String,
    pub file_count: i64,
    /// Earliest timestamp across all files in this stream (microseconds).
    pub min_ts: i64,
    /// Latest timestamp across all files in this stream (microseconds).
    pub max_ts: i64,
    pub total_records: i64,
    pub total_original_size: i64,
    pub total_compressed_size: i64,
    pub updated_at: i64,
}

/// Input for upserting stream stats after writing a new Parquet file.
#[derive(Debug, Clone)]
pub struct StreamStatsUpdate {
    pub workspace_id: WorkspaceId,
    pub signal_type: String,
    pub stream_name: String,
    /// Minimum timestamp of the newly written file (microseconds).
    pub min_ts: i64,
    /// Maximum timestamp of the newly written file (microseconds).
    pub max_ts: i64,
    pub records_delta: i64,
    pub original_size_delta: i64,
    pub compressed_size_delta: i64,
}

/// Filter for querying the file list.
#[derive(Debug, Clone, Default)]
pub struct FileListFilter {
    pub workspace_id: Option<Uuid>,
    /// Signal type: "traces", "metrics", or "logs".
    pub signal_type: Option<String>,
    pub stream_name: Option<String>,
    /// Query window start (microseconds). Keeps files where `max_ts >= start`.
    pub time_range_start: Option<i64>,
    /// Query window end (microseconds). Keeps files where `min_ts <= end`.
    pub time_range_end: Option<i64>,
    /// Storage location filter: "local" or "s3".
    pub location: Option<String>,
    /// When `None`, returns all files regardless of deletion state.
    pub deleted: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_list_filter_default() {
        let f = FileListFilter::default();
        assert!(f.workspace_id.is_none());
        assert!(f.signal_type.is_none());
        assert!(f.deleted.is_none());
    }

    #[test]
    fn test_stream_stats_update_fields() {
        let u = StreamStatsUpdate {
            workspace_id: WorkspaceId::new(),
            signal_type: "traces".to_string(),
            stream_name: "my-service".to_string(),
            min_ts: 1_000_000,
            max_ts: 2_000_000,
            records_delta: 100,
            original_size_delta: 50_000,
            compressed_size_delta: 20_000,
        };
        assert_eq!(u.signal_type, "traces");
        assert_eq!(u.records_delta, 100);
    }
}
