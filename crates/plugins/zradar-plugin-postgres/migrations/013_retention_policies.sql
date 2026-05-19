CREATE TABLE IF NOT EXISTS retention_policies (
    id BIGSERIAL PRIMARY KEY,
    org_id UUID NOT NULL UNIQUE,
    default_days INTEGER NOT NULL,
    project_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retention_policies_org_id ON retention_policies(org_id);
