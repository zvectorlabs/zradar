-- Migration: Add WAL replay offset column to file_list (Phase 08)
--
-- Tracks which WAL offset produced each Parquet file, enabling idempotent
-- replay after crash recovery.

ALTER TABLE file_list ADD COLUMN IF NOT EXISTS wal_replay_offset BIGINT NULL;

CREATE INDEX IF NOT EXISTS idx_file_list_wal_offset
    ON file_list (tenant_id, project_id, signal_type, stream_name, wal_replay_offset)
    WHERE wal_replay_offset IS NOT NULL;
