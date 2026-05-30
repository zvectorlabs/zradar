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
