-- Initial schema for zradar.
--
-- Only two tables are needed:
--   file_list   — tracks every Parquet file written to local disk or S3
--   stream_stats — per-stream aggregated statistics for fast overview queries

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
    original_size    BIGINT      NOT NULL DEFAULT 0,
    compressed_size  BIGINT      NOT NULL DEFAULT 0,
    deleted          BOOLEAN     NOT NULL DEFAULT false,
    created_at       BIGINT      NOT NULL,
    updated_at       BIGINT      NOT NULL
);

-- Primary query index: covers query_files() WHERE + ORDER BY
CREATE INDEX IF NOT EXISTS idx_file_list_query
    ON file_list (tenant_id, project_id, signal_type, deleted, max_ts, min_ts);

-- FileMover index: partial index on local files ordered by age
CREATE INDEX IF NOT EXISTS idx_file_list_mover_v2
    ON file_list (deleted, created_at DESC)
    WHERE location = 'local';

-- Compactor index: group-by key for merge candidate selection
CREATE INDEX IF NOT EXISTS idx_file_list_compactor
    ON file_list (tenant_id, project_id, signal_type, date, deleted)
    WHERE deleted = false;

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
