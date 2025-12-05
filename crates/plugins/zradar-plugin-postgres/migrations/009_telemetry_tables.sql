-- Telemetry data tables (traces, spans, metrics)
-- Matching ClickHouse schema for consistency across storage backends

-- ============================================================================
-- Spans table with LLM-specific fields (matching ClickHouse schema)
-- ============================================================================
CREATE TABLE IF NOT EXISTS spans (
    -- Identity
    trace_id VARCHAR(32) NOT NULL,
    span_id VARCHAR(16) NOT NULL,
    parent_span_id VARCHAR(16) DEFAULT '',
    
    -- Timing
    timestamp BIGINT NOT NULL,               -- Unix nanoseconds
    duration_ns BIGINT NOT NULL,
    
    -- Hierarchy (Two-Level Multi-tenancy)
    tenant_id VARCHAR(255) NOT NULL,         -- Organization/Team
    project_id VARCHAR(255) NOT NULL,        -- Project within org
    
    -- Service Metadata
    service_name VARCHAR(255) NOT NULL,
    span_name VARCHAR(500) NOT NULL,
    span_kind VARCHAR(50) DEFAULT 'INTERNAL',  -- INTERNAL, CLIENT, SERVER, etc.
    span_type VARCHAR(16) DEFAULT 'SPAN' NOT NULL,  -- SPAN, EVENT, GENERATION, AGENT, TOOL, etc.
    
    -- Status
    status_code VARCHAR(50) DEFAULT 'UNSET',   -- UNSET, OK, ERROR
    status_message TEXT DEFAULT '',
    
    -- Agent Context (Commonly Queried)
    invocation_id VARCHAR(255) DEFAULT '',
    session_id VARCHAR(255) DEFAULT '',
    user_id VARCHAR(255) DEFAULT '',
    agent_name VARCHAR(255) DEFAULT '',
    agent_type VARCHAR(100) DEFAULT '',
    
    -- LLM-Specific Fields (stored as JSONB for searchability)
    llm_model VARCHAR(255) DEFAULT '',
    llm_input JSONB,                           -- LLM prompt (JSONB for querying)
    llm_output JSONB,                          -- LLM completion (JSONB for querying)
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    
    -- Cost Tracking
    prompt_cost_usd DOUBLE PRECISION DEFAULT 0.0,
    completion_cost_usd DOUBLE PRECISION DEFAULT 0.0,
    total_cost_usd DOUBLE PRECISION DEFAULT 0.0,
    
    -- Tool-Specific
    tool_name VARCHAR(255) DEFAULT '',
    tool_call_id VARCHAR(255) DEFAULT '',
    
    -- Resource Attributes (From Profiling)
    resource_cpu_micros BIGINT DEFAULT 0,
    resource_memory_bytes BIGINT DEFAULT 0,
    resource_memory_peak BIGINT DEFAULT 0,
    
    -- Prompt Management
    prompt_id VARCHAR(255) DEFAULT '',
    prompt_name VARCHAR(255) DEFAULT '',
    prompt_version INTEGER DEFAULT 0,
    
    -- Timing Details
    completion_start_time BIGINT,              -- Nullable
    time_to_first_token_ms INTEGER DEFAULT 0,
    
    -- Versioning
    agent_version VARCHAR(100) DEFAULT '',
    sdk_version VARCHAR(100) DEFAULT '',
    
    -- Level/Severity
    level VARCHAR(50) DEFAULT 'INFO',          -- DEBUG, INFO, WARNING, ERROR, CRITICAL
    
    -- Flexible Attributes (JSONB for searchability)
    model_parameters JSONB,                    -- Model parameters JSON: {"temperature": 0.7, ...}
    attributes JSONB,                           -- All other key-value pairs (JSONB for querying)
    
    -- Record Lifecycle
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    is_deleted SMALLINT DEFAULT 0,
    
    -- Primary key and uniqueness
    PRIMARY KEY (tenant_id, project_id, span_id)
);

-- Essential indexes only (minimized for storage efficiency)
-- Primary key already covers (tenant_id, project_id, span_id)
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(tenant_id, trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_timestamp ON spans(timestamp DESC);


-- ============================================================================
-- Metrics table (matching ClickHouse schema)
-- ============================================================================
CREATE TABLE IF NOT EXISTS metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identity
    metric_name VARCHAR(500) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,          -- COUNTER, GAUGE, HISTOGRAM, SUMMARY
    
    -- Timing
    timestamp BIGINT NOT NULL,
    
    -- Hierarchy
    tenant_id VARCHAR(255) NOT NULL,
    project_id VARCHAR(255) NOT NULL,
    
    -- Values
    value DOUBLE PRECISION,
    count BIGINT DEFAULT 0,
    sum DOUBLE PRECISION DEFAULT 0,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    
    -- Common labels
    service_name VARCHAR(255) NOT NULL DEFAULT '',
    agent_name VARCHAR(255) DEFAULT '',
    user_id VARCHAR(255) DEFAULT '',
    session_id VARCHAR(255) DEFAULT '',
    
    -- All other labels as JSON
    labels TEXT DEFAULT '{}'
);

-- Essential indexes only for metrics
CREATE INDEX IF NOT EXISTS idx_metrics_lookup ON metrics(tenant_id, project_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp DESC);
