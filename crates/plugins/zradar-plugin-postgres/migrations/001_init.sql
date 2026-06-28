-- Migration squashed from 001_init.sql
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


-- Migration squashed from 012_project_settings.sql
CREATE TABLE IF NOT EXISTS project_settings (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL UNIQUE,
    traces_retention_days INTEGER NOT NULL DEFAULT 90,
    metrics_retention_days INTEGER NOT NULL DEFAULT 30,
    logs_retention_days INTEGER NOT NULL DEFAULT 30,
    max_ingestion_rate INTEGER NULL,
    file_push_interval_secs INTEGER NOT NULL DEFAULT 300,
    blocked BOOLEAN NOT NULL DEFAULT false,
    updated_at BIGINT NOT NULL
);


-- Migration squashed from 013_retention_policies.sql
CREATE TABLE IF NOT EXISTS retention_policies (
    id BIGSERIAL PRIMARY KEY,
    org_id UUID NOT NULL UNIQUE,
    default_days INTEGER NOT NULL,
    project_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retention_policies_org_id ON retention_policies(org_id);


-- Migration squashed from 014_audit_logs.sql
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    actor_tenant_id UUID NULL,
    actor_project_id UUID NULL,
    org_id UUID NULL,
    project_id UUID NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_org_id_created_at ON audit_logs(org_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_project_id_created_at ON audit_logs(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs(resource_type, resource_id);


-- Migration squashed from 015_file_list_indexes.sql
CREATE INDEX IF NOT EXISTS idx_file_list_query
    ON file_list (tenant_id, project_id, signal_type, deleted, max_ts, min_ts);

CREATE INDEX IF NOT EXISTS idx_file_list_mover
    ON file_list (location, deleted, created_at DESC)
    WHERE location = 'local';

CREATE INDEX IF NOT EXISTS idx_file_list_compactor
    ON file_list (tenant_id, project_id, signal_type, date, deleted);


-- Migration squashed from 016_file_list_wal_offset.sql
-- Migration: Add WAL replay offset column to file_list (Phase 08)
--
-- Tracks which WAL offset produced each Parquet file, enabling idempotent
-- replay after crash recovery.

ALTER TABLE file_list ADD COLUMN IF NOT EXISTS wal_replay_offset BIGINT NULL;

CREATE INDEX IF NOT EXISTS idx_file_list_wal_offset
    ON file_list (tenant_id, project_id, signal_type, stream_name, wal_replay_offset)
    WHERE wal_replay_offset IS NOT NULL;


-- Migration squashed from 017_policies.sql
CREATE TABLE IF NOT EXISTS policies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    project_id UUID NULL,
    signal_kind TEXT NOT NULL,
    operation TEXT NOT NULL,
    limit_kind TEXT NOT NULL,
    limit_json JSONB NOT NULL,
    grace_pct SMALLINT NOT NULL DEFAULT 101,
    hard_block_pct SMALLINT NOT NULL DEFAULT 103,
    effective_from BIGINT NOT NULL,
    effective_until BIGINT NULL,
    source TEXT NOT NULL DEFAULT 'api',
    updated_at BIGINT NOT NULL,
    CHECK (grace_pct >= 0 AND grace_pct <= 255),
    CHECK (hard_block_pct >= 0 AND hard_block_pct <= 255),
    CHECK (effective_until IS NULL OR effective_until > effective_from)
);

CREATE INDEX IF NOT EXISTS idx_policies_tenant_lookup
    ON policies (tenant_id, project_id, signal_kind, operation);

CREATE INDEX IF NOT EXISTS idx_policies_effective
    ON policies (tenant_id, effective_from, effective_until);

CREATE UNIQUE INDEX IF NOT EXISTS idx_policies_tenant_default_active_unique
    ON policies (tenant_id, signal_kind, operation, limit_kind)
    WHERE project_id IS NULL AND effective_until IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_policies_project_active_unique
    ON policies (tenant_id, project_id, signal_kind, operation, limit_kind)
    WHERE project_id IS NOT NULL AND effective_until IS NULL;


-- Migration squashed from 018_usage_events.sql
CREATE TABLE IF NOT EXISTS ingestion_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    stream_name TEXT NULL,
    compressed_bytes BIGINT NOT NULL,
    original_bytes BIGINT NULL,
    records BIGINT NOT NULL,
    file_id BIGINT NULL REFERENCES file_list(id),
    decision TEXT NOT NULL,
    flushed_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingestion_events_lookup
    ON ingestion_events (tenant_id, project_id, flushed_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_events_period
    ON ingestion_events (tenant_id, project_id, signal_kind, flushed_at);

CREATE TABLE IF NOT EXISTS query_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    bytes_scanned BIGINT NOT NULL,
    rows_scanned BIGINT NULL,
    query_time_ms INTEGER NULL,
    decision TEXT NOT NULL,
    submitted_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_query_events_lookup
    ON query_events (tenant_id, project_id, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_query_events_period
    ON query_events (tenant_id, project_id, signal_kind, submitted_at);


-- Migration squashed from 019_usage_rollups.sql
CREATE TABLE IF NOT EXISTS ingestion_daily (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    day DATE NOT NULL,
    compressed_bytes BIGINT NOT NULL DEFAULT 0,
    original_bytes BIGINT NOT NULL DEFAULT 0,
    records BIGINT NOT NULL DEFAULT 0,
    file_count BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    gb INTEGER GENERATED ALWAYS AS (((compressed_bytes + 1073741823) / 1073741824)::integer) STORED,
    PRIMARY KEY (tenant_id, project_id, signal_kind, day)
);

CREATE TABLE IF NOT EXISTS query_usage_daily (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    day DATE NOT NULL,
    bytes_scanned BIGINT NOT NULL DEFAULT 0,
    rows_scanned BIGINT NOT NULL DEFAULT 0,
    query_count BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    gb_scanned INTEGER GENERATED ALWAYS AS (((bytes_scanned + 1073741823) / 1073741824)::integer) STORED,
    PRIMARY KEY (tenant_id, project_id, signal_kind, day)
);

CREATE TABLE IF NOT EXISTS retention_storage_daily (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    bucket_index INTEGER NOT NULL,
    day DATE NOT NULL,
    compressed_bytes BIGINT NOT NULL DEFAULT 0,
    file_count BIGINT NOT NULL DEFAULT 0,
    captured_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, project_id, signal_kind, bucket_index, day)
);

CREATE TABLE IF NOT EXISTS ingest_query_monthly (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    operation TEXT NOT NULL,
    period_start DATE NOT NULL,
    used_bytes BIGINT NOT NULL DEFAULT 0,
    limit_bytes BIGINT NOT NULL,
    last_breach_at BIGINT NULL,
    updated_at BIGINT NOT NULL,
    used_gb INTEGER GENERATED ALWAYS AS (((used_bytes + 1073741823) / 1073741824)::integer) STORED,
    limit_gb INTEGER GENERATED ALWAYS AS (((limit_bytes + 1073741823) / 1073741824)::integer) STORED,
    PRIMARY KEY (tenant_id, project_id, signal_kind, operation, period_start)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_daily_lookup
    ON ingestion_daily (tenant_id, project_id, day DESC);

CREATE INDEX IF NOT EXISTS idx_query_usage_daily_lookup
    ON query_usage_daily (tenant_id, project_id, day DESC);

CREATE INDEX IF NOT EXISTS idx_retention_storage_daily_lookup
    ON retention_storage_daily (tenant_id, project_id, day DESC);

CREATE INDEX IF NOT EXISTS idx_ingest_query_monthly_lookup
    ON ingest_query_monthly (tenant_id, project_id, operation, period_start DESC);

CREATE INDEX IF NOT EXISTS idx_file_list_active_size
    ON file_list (tenant_id, project_id, signal_type)
    INCLUDE (compressed_size)
    WHERE deleted = false;


-- Migration squashed from 020_policy_audit_thresholds.sql
CREATE TABLE IF NOT EXISTS policy_decisions_audit (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    operation TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    observed_value BIGINT NULL,
    limit_value BIGINT NULL,
    block_code TEXT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_policy_decisions_audit_lookup
    ON policy_decisions_audit (tenant_id, project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS threshold_dedupe (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    operation TEXT NOT NULL,
    limit_kind TEXT NOT NULL,
    threshold_pct SMALLINT NOT NULL,
    period_start BIGINT NOT NULL,
    emitted_at BIGINT NOT NULL,
    PRIMARY KEY (
        tenant_id,
        project_id,
        signal_kind,
        operation,
        limit_kind,
        threshold_pct,
        period_start
    )
);

CREATE INDEX IF NOT EXISTS idx_threshold_dedupe_emitted_at
    ON threshold_dedupe (emitted_at);


-- Migration squashed from 021_ingestion_events_file_id_on_delete_set_null.sql
ALTER TABLE ingestion_events
    DROP CONSTRAINT IF EXISTS ingestion_events_file_id_fkey;

ALTER TABLE ingestion_events
    ADD CONSTRAINT ingestion_events_file_id_fkey
    FOREIGN KEY (file_id)
    REFERENCES file_list(id)
    ON DELETE SET NULL;


-- Migration squashed from 022_storage_cleanup_daily.sql
CREATE TABLE IF NOT EXISTS storage_cleanup_daily (
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    signal_kind TEXT NOT NULL,
    day DATE NOT NULL,
    compressed_bytes BIGINT NOT NULL DEFAULT 0,
    file_count BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, project_id, signal_kind, day)
);

CREATE INDEX IF NOT EXISTS idx_storage_cleanup_daily_lookup
    ON storage_cleanup_daily (tenant_id, project_id, day DESC);


-- Migration squashed from 023_file_list_active_keys_index.sql
-- Index for StorageUsageDailyJob::list_active_keys().
--
-- The query is:
--   SELECT DISTINCT tenant_id, project_id, signal_type
--   FROM file_list
--   WHERE deleted = false AND created_at < $1
--
-- The existing idx_file_list_active_size covers (tenant_id, project_id, signal_type)
-- WHERE deleted = false but does not include created_at, forcing a heap fetch to
-- evaluate the created_at filter. This index puts created_at first so Postgres can
-- do a range scan and read all projected columns from the index alone (index-only scan).
CREATE INDEX IF NOT EXISTS idx_file_list_active_keys
    ON file_list (created_at, tenant_id, project_id, signal_type)
    WHERE deleted = false;


-- Migration squashed from 024_project_settings_capture_llm_content.sql
-- Store project-level LLM content capture policy.
ALTER TABLE project_settings
ADD COLUMN IF NOT EXISTS capture_llm_content_enabled BOOLEAN NOT NULL DEFAULT TRUE;


-- Migration squashed from 025_workspace_scope.sql
-- Destructive workspace scope migration
-- Apply to empty or backed up DB only

-- 1. file_list
ALTER TABLE file_list
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id;

ALTER TABLE file_list
  ADD COLUMN workspace_id UUID NOT NULL;

CREATE INDEX idx_file_list_workspace_signal
  ON file_list (workspace_id, signal_type, deleted, max_ts);

-- 2. stream_stats
ALTER TABLE stream_stats
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id;

ALTER TABLE stream_stats
  ADD COLUMN workspace_id UUID NOT NULL;

ALTER TABLE stream_stats ADD UNIQUE (workspace_id, signal_type, stream_name);

-- 3. workspace_settings (was project_settings)
ALTER TABLE project_settings RENAME TO workspace_settings;
ALTER TABLE workspace_settings RENAME COLUMN project_id TO workspace_id;

-- 4. retention_policies
DROP TABLE IF EXISTS retention_policies;

-- 5. policies and usage tables
ALTER TABLE policies
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL;
CREATE INDEX idx_policies_workspace_lookup ON policies (workspace_id, signal_kind, operation);
CREATE INDEX idx_policies_workspace_effective ON policies (workspace_id, effective_from, effective_until);
CREATE UNIQUE INDEX idx_policies_workspace_active_unique ON policies (workspace_id, signal_kind, operation, limit_kind) WHERE effective_until IS NULL;

ALTER TABLE ingestion_events
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL;
CREATE INDEX idx_ingestion_events_lookup ON ingestion_events (workspace_id, flushed_at DESC);
CREATE INDEX idx_ingestion_events_period ON ingestion_events (workspace_id, signal_kind, flushed_at);

ALTER TABLE query_events
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL;
CREATE INDEX idx_query_events_lookup ON query_events (workspace_id, submitted_at DESC);
CREATE INDEX idx_query_events_period ON query_events (workspace_id, signal_kind, submitted_at);

ALTER TABLE ingestion_daily
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, day);
CREATE INDEX idx_ingestion_daily_lookup ON ingestion_daily (workspace_id, day DESC);

ALTER TABLE query_usage_daily
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, day);
CREATE INDEX idx_query_usage_daily_lookup ON query_usage_daily (workspace_id, day DESC);

ALTER TABLE retention_storage_daily
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, bucket_index, day);
CREATE INDEX idx_retention_storage_daily_lookup ON retention_storage_daily (workspace_id, day DESC);

ALTER TABLE ingest_query_monthly
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, operation, period_start);
CREATE INDEX idx_ingest_query_monthly_lookup ON ingest_query_monthly (workspace_id, operation, period_start DESC);

ALTER TABLE storage_cleanup_daily
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, day);
CREATE INDEX idx_storage_cleanup_daily_lookup ON storage_cleanup_daily (workspace_id, day DESC);

ALTER TABLE policy_decisions_audit
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL;
CREATE INDEX idx_policy_decisions_audit_lookup ON policy_decisions_audit (workspace_id, created_at DESC);

ALTER TABLE threshold_dedupe
  DROP COLUMN IF EXISTS tenant_id,
  DROP COLUMN IF EXISTS project_id,
  ADD COLUMN workspace_id UUID NOT NULL,
  ADD PRIMARY KEY (workspace_id, signal_kind, operation, limit_kind, threshold_pct, period_start);

-- 6. audit_logs
ALTER TABLE audit_logs
  DROP COLUMN IF EXISTS actor_tenant_id,
  DROP COLUMN IF EXISTS actor_project_id,
  DROP COLUMN IF EXISTS org_id,
  DROP COLUMN IF EXISTS project_id;

ALTER TABLE audit_logs
  ADD COLUMN actor_workspace_id UUID,
  ADD COLUMN resource_workspace_id UUID;


