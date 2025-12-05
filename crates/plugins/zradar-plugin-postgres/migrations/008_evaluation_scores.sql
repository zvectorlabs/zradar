-- Evaluation scores table for LLM/trace evaluations

-- ============================================================================
-- Evaluation Scores
-- ============================================================================
CREATE TABLE IF NOT EXISTS evaluation_scores (
    id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    project_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    event_ts BIGINT NOT NULL,
    trace_id VARCHAR(255) NOT NULL,
    span_id VARCHAR(16) DEFAULT '',
    session_id VARCHAR(255) DEFAULT '',
    dataset_run_id VARCHAR(255) DEFAULT '',
    name VARCHAR(255) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    string_value TEXT DEFAULT '',
    source VARCHAR(50) NOT NULL,
    comment TEXT DEFAULT '',
    author_user_id VARCHAR(255) DEFAULT '',
    config_id VARCHAR(255) DEFAULT '',
    eval_execution_trace_id VARCHAR(255) DEFAULT '',
    queue_id VARCHAR(255) DEFAULT '',
    environment VARCHAR(100) DEFAULT 'default',
    service_name VARCHAR(255) DEFAULT '',
    agent_name VARCHAR(255) DEFAULT '',
    user_id VARCHAR(255) DEFAULT '',
    metadata TEXT DEFAULT '{}',
    is_deleted SMALLINT DEFAULT 0
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_eval_scores_tenant_project ON evaluation_scores(tenant_id, project_id);
CREATE INDEX IF NOT EXISTS idx_eval_scores_trace ON evaluation_scores(tenant_id, project_id, trace_id);
CREATE INDEX IF NOT EXISTS idx_eval_scores_session ON evaluation_scores(tenant_id, project_id, session_id);
CREATE INDEX IF NOT EXISTS idx_eval_scores_timestamp ON evaluation_scores(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_eval_scores_name ON evaluation_scores(name);

