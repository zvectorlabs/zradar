-- Job queue for telemetry ingestion

-- ============================================================================
-- Job Queue (for telemetry ingestion)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' NOT NULL,
    file_path TEXT NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    project_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    retry_count INTEGER DEFAULT 0 NOT NULL,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON ingestion_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON ingestion_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_pending ON ingestion_jobs(status, created_at) WHERE status = 'pending';

