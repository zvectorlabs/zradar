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
