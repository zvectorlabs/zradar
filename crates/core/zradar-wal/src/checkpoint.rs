/// Checkpoint persistence for the WAL flusher.
///
/// Tracks the last offset + segment that was fully flushed to Parquet.
/// Written atomically via tmp+fsync+rename so a crash never corrupts it.
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// On-disk checkpoint state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// Segment id of the last fully flushed record.
    pub last_flushed_segment_id: u64,
    /// Offset of the last record that was flushed and confirmed in Parquet.
    pub last_flushed_offset: u64,
    /// WAL format version (for future-proofing).
    pub wal_format_version: u8,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            last_flushed_segment_id: 0,
            last_flushed_offset: 0,
            wal_format_version: 1,
        }
    }
}

/// Manages reading/writing the checkpoint file.
pub struct CheckpointStore {
    path: PathBuf,
}

impl CheckpointStore {
    /// Create a store pointing at the given checkpoint path.
    pub fn new(wal_dir: &Path) -> Self {
        Self {
            path: wal_dir.join("checkpoint.json"),
        }
    }

    /// Load the checkpoint from disk. Returns `None` if the file does not exist
    /// (i.e., nothing has ever been flushed).
    pub fn load(&self) -> anyhow::Result<Option<Checkpoint>> {
        match std::fs::read_to_string(&self.path) {
            Ok(contents) => {
                let cp: Checkpoint = serde_json::from_str(&contents)?;
                Ok(Some(cp))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist the checkpoint atomically: write tmp → fsync → rename.
    pub fn save(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let data = serde_json::to_string_pretty(checkpoint)?;

        std::fs::write(&tmp_path, data.as_bytes())?;

        // fsync the temp file
        let file = std::fs::File::open(&tmp_path)?;
        file.sync_all()?;
        drop(file);

        // Atomic rename
        std::fs::rename(&tmp_path, &self.path)?;

        Ok(())
    }

    /// Path to the checkpoint file (for testing).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = CheckpointStore::new(tmp.path());

        let cp = Checkpoint {
            last_flushed_segment_id: 5,
            last_flushed_offset: 12345,
            wal_format_version: 1,
        };

        store.save(&cp).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, cp);
    }

    #[test]
    fn test_checkpoint_missing_file_returns_none() {
        let tmp = TempDir::new().unwrap();
        let store = CheckpointStore::new(tmp.path());
        let cp = store.load().unwrap();
        assert!(cp.is_none());
    }

    #[test]
    fn test_checkpoint_atomic_write_no_corruption() {
        let tmp = TempDir::new().unwrap();
        let store = CheckpointStore::new(tmp.path());

        // Write multiple times; the final one should be visible
        for i in 0..10 {
            store
                .save(&Checkpoint {
                    last_flushed_segment_id: i,
                    last_flushed_offset: i * 100,
                    wal_format_version: 1,
                })
                .unwrap();
        }

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.last_flushed_segment_id, 9);
        assert_eq!(loaded.last_flushed_offset, 900);
    }
}
