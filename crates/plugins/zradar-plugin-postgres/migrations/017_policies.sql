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
