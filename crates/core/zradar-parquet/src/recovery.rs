//! M07-03: Startup crash recovery — remove orphaned `.par` temp files.
//!
//! The atomic write sequence creates `<uuid>.par`, then renames it to
//! `<uuid>.parquet`. A crash between write and rename leaves a `.par` orphan
//! that can never be promoted. This function removes all such files on startup.

use anyhow::Context;
use std::path::Path;
use tracing::{info, warn};

/// Walk `data_dir` recursively and delete any orphaned `.par` temp files.
///
/// Call this once at server startup, before accepting any write traffic.
pub async fn recover_incomplete_writes(data_dir: &Path) -> anyhow::Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }

    let data_dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut removed = 0usize;
        walk_and_delete(&data_dir, &mut removed);
        if removed > 0 {
            info!(
                count = removed,
                dir = %data_dir.display(),
                "Crash recovery: removed orphaned .par temp files"
            );
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("spawn_blocking panicked in recover_incomplete_writes")?
}

fn walk_and_delete(dir: &Path, removed: &mut usize) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            warn!(dir = %dir.display(), error = %err, "Failed to read directory during crash recovery");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_and_delete(&path, removed);
        } else if path.extension().map(|e| e == "par").unwrap_or(false) {
            warn!(
                path = %path.display(),
                "Removing orphaned .par temp file (crash recovery)"
            );
            match std::fs::remove_file(&path) {
                Ok(()) => *removed += 1,
                Err(err) => {
                    warn!(path = %path.display(), error = %err, "Failed to remove orphaned .par file");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recover_removes_par_files() {
        let dir = TempDir::new().unwrap();

        // Create a .par orphan and a valid .parquet file side-by-side.
        let par_path = dir.path().join("orphan.par");
        let parquet_path = dir.path().join("valid.parquet");
        std::fs::write(&par_path, b"incomplete").unwrap();
        std::fs::write(&parquet_path, b"complete").unwrap();

        recover_incomplete_writes(dir.path()).await.unwrap();

        assert!(!par_path.exists(), ".par file must be deleted");
        assert!(parquet_path.exists(), ".parquet file must be preserved");
    }

    #[tokio::test]
    async fn test_recover_handles_nested_directories() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("files/tenant/traces/svc/2024/01/01/00");
        std::fs::create_dir_all(&sub).unwrap();

        let par_path = sub.join("abc.par");
        std::fs::write(&par_path, b"bad").unwrap();

        recover_incomplete_writes(dir.path()).await.unwrap();

        assert!(!par_path.exists(), "nested .par file must be deleted");
    }

    #[tokio::test]
    async fn test_recover_noop_when_dir_missing() {
        let result =
            recover_incomplete_writes(Path::new("/tmp/nonexistent_zradar_test_dir_xyz")).await;
        assert!(
            result.is_ok(),
            "must not error when data_dir does not exist"
        );
    }
}
