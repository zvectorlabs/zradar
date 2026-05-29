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
