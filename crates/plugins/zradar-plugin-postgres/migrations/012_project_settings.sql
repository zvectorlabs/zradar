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
