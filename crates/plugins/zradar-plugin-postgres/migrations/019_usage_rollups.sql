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
