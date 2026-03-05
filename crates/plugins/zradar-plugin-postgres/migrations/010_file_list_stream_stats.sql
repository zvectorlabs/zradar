-- Migration 010: Parquet file list and stream statistics tables
--
-- file_list tracks every Parquet file written to local disk or S3.
-- stream_stats holds per-stream aggregated statistics for fast overview queries.

-- ---------------------------------------------------------------------------
-- file_list
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS file_list (
    id               BIGSERIAL  PRIMARY KEY,
    tenant_id        UUID        NOT NULL,
    project_id       UUID        NOT NULL,
    signal_type      TEXT        NOT NULL,          -- traces | metrics | logs
    stream_name      TEXT        NOT NULL,
    date             TEXT        NOT NULL,          -- YYYY/MM/DD/HH
    file_path        TEXT        NOT NULL,          -- local path or S3 key
    location         TEXT        NOT NULL DEFAULT 'local',  -- local | s3
    min_ts           BIGINT      NOT NULL,          -- microseconds since epoch
    max_ts           BIGINT      NOT NULL,          -- microseconds since epoch
    records          BIGINT      NOT NULL DEFAULT 0,
    original_size    BIGINT      NOT NULL DEFAULT 0,   -- uncompressed bytes estimate
    compressed_size  BIGINT      NOT NULL DEFAULT 0,   -- actual Parquet file size
    deleted          BOOLEAN     NOT NULL DEFAULT false,
    created_at       BIGINT      NOT NULL,          -- microseconds since epoch
    updated_at       BIGINT      NOT NULL           -- microseconds since epoch
);

-- Primary lookup: tenant + project + signal type + stream + date partition
CREATE INDEX IF NOT EXISTS idx_file_list_lookup
    ON file_list (tenant_id, project_id, signal_type, stream_name, date);

-- Time range pruning: find files whose time range overlaps a query window
CREATE INDEX IF NOT EXISTS idx_file_list_time_range
    ON file_list (min_ts, max_ts);

-- FileMover and RetentionJob: find files by storage location and deletion state
CREATE INDEX IF NOT EXISTS idx_file_list_mover
    ON file_list (location, deleted);

-- ---------------------------------------------------------------------------
-- stream_stats
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_stats (
    id                    BIGSERIAL  PRIMARY KEY,
    tenant_id             UUID       NOT NULL,
    project_id            UUID       NOT NULL,
    signal_type           TEXT       NOT NULL,
    stream_name           TEXT       NOT NULL,
    file_count            BIGINT     NOT NULL DEFAULT 0,
    min_ts                BIGINT     NOT NULL DEFAULT 0,
    max_ts                BIGINT     NOT NULL DEFAULT 0,
    total_records         BIGINT     NOT NULL DEFAULT 0,
    total_original_size   BIGINT     NOT NULL DEFAULT 0,
    total_compressed_size BIGINT     NOT NULL DEFAULT 0,
    updated_at            BIGINT     NOT NULL,

    UNIQUE (tenant_id, project_id, signal_type, stream_name)
);
