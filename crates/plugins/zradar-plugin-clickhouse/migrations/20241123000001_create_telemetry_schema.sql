-- ClickHouse Schema for zradar
-- Adding Tables
-- Note: This migration expects the database to already exist
-- and be specified via the --database flag

-- Spans table with LLM-specific fields
CREATE TABLE IF NOT EXISTS spans (
    -- Identity
    trace_id String,
    span_id String,
    parent_span_id String,
    
    -- Timing
    timestamp DateTime64(9, 'UTC'),
    duration_ns UInt64,
    
    -- Hierarchy (two-level tenancy) - UUIDs from PostgreSQL
    tenant_id LowCardinality(String),      -- organization_id from PostgreSQL
    project_id LowCardinality(String),     -- project_id from PostgreSQL
    
    -- Service metadata
    service_name LowCardinality(String),
    span_name LowCardinality(String),
    span_kind LowCardinality(String),
    span_type LowCardinality(String) DEFAULT 'SPAN',
    
    -- Status
    status_code LowCardinality(String),
    status_message String,
    
    -- Agent context (commonly queried)
    invocation_id String,
    session_id String,
    user_id String,
    agent_name LowCardinality(String),
    agent_type LowCardinality(String),
    
    -- LLM-specific fields
    llm_model LowCardinality(String),
    llm_input String CODEC(ZSTD(1)),           -- Compressed
    llm_output String CODEC(ZSTD(1)),          -- Compressed
    prompt_tokens UInt32,
    completion_tokens UInt32,
    total_tokens UInt32,
    
    -- Cost tracking
    prompt_cost_usd Float64,
    completion_cost_usd Float64,
    total_cost_usd Float64,
    
    -- Tool-specific
    tool_name LowCardinality(String),
    tool_call_id String,
    
    -- Resource attributes (from profiling)
    resource_cpu_micros UInt64,
    resource_memory_bytes UInt64,
    resource_memory_peak UInt64,
    
    -- Prompt management
    prompt_id String,
    prompt_name LowCardinality(String),
    prompt_version UInt32,
    
    -- Timing details
    completion_start_time Nullable(DateTime64(9, 'UTC')),
    time_to_first_token_ms UInt32,
    
    -- Versioning
    agent_version LowCardinality(String),
    sdk_version LowCardinality(String),
    
    -- Level/Severity
    level LowCardinality(String),
    
    -- JSON fields (for flexibility)
    model_parameters String CODEC(ZSTD(1)),
    attributes String CODEC(ZSTD(1)),
    
    -- Record lifecycle
    created_at DateTime64(9, 'UTC'),
    updated_at DateTime64(9, 'UTC'),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, project_id, span_type, timestamp, trace_id, span_id)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Note: Indexes cannot be added idempotently in ClickHouse init scripts
-- They should be added manually after table creation or via migrations
-- Uncomment these lines only for fresh database creation:
-- 
-- ALTER TABLE spans ADD INDEX idx_trace_id trace_id TYPE bloom_filter GRANULARITY 4;
-- ALTER TABLE spans ADD INDEX idx_session_id session_id TYPE bloom_filter GRANULARITY 4;
-- ALTER TABLE spans ADD INDEX idx_user_id user_id TYPE bloom_filter GRANULARITY 4;
-- ALTER TABLE spans ADD INDEX idx_agent_name agent_name TYPE set(100) GRANULARITY 4;
-- ALTER TABLE spans ADD INDEX idx_llm_model llm_model TYPE set(50) GRANULARITY 4;

-- Metrics table
CREATE TABLE IF NOT EXISTS metrics (
    -- Identity
    metric_name LowCardinality(String),
    metric_type Enum8('COUNTER' = 1, 'GAUGE' = 2, 'HISTOGRAM' = 3, 'SUMMARY' = 4),
    
    -- Timing
    timestamp DateTime64(9, 'UTC'),
    
    -- Hierarchy - UUIDs from PostgreSQL
    tenant_id LowCardinality(String),
    project_id LowCardinality(String),
    
    -- Values
    value Float64,
    count UInt64,
    sum Float64,
    min Float64,
    max Float64,
    
    -- Common labels
    service_name LowCardinality(String),
    agent_name LowCardinality(String),
    user_id String,
    session_id String,
    
    -- All other labels as JSON
    labels String CODEC(ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, project_id, metric_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Materialized view for cost summary by project
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_project_costs
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tenant_id, project_id, date, llm_model)
AS
SELECT
    tenant_id,
    project_id,
    toDate(timestamp) AS date,
    llm_model,
    count() AS span_count,
    sum(prompt_tokens) AS total_prompt_tokens,
    sum(completion_tokens) AS total_completion_tokens,
    sum(total_tokens) AS total_tokens,
    sum(total_cost_usd) AS total_cost_usd
FROM spans
WHERE llm_model != ''
GROUP BY tenant_id, project_id, date, llm_model;

-- Materialized view for agent performance summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agent_performance
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tenant_id, project_id, agent_name, date)
AS
SELECT
    tenant_id,
    project_id,
    agent_name,
    toDate(timestamp) AS date,
    count() AS invocation_count,
    avgState(duration_ns) AS avg_duration_ns,
    quantileState(0.5)(duration_ns) AS p50_duration_ns,
    quantileState(0.95)(duration_ns) AS p95_duration_ns,
    quantileState(0.99)(duration_ns) AS p99_duration_ns
FROM spans
WHERE span_kind = 'INTERNAL' AND agent_name != ''
GROUP BY tenant_id, project_id, agent_name, date;

-- ============================================================================
-- Evaluation Scores Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS evaluation_scores (
    -- Identity
    id String,
    tenant_id LowCardinality(String),
    project_id LowCardinality(String),
    
    -- Timing
    timestamp DateTime64(3, 'UTC'),
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    event_ts DateTime64(3, 'UTC'),
    
    -- Entity Association 
    trace_id String,
    span_id String DEFAULT '',
    session_id String DEFAULT '',
    dataset_run_id String DEFAULT '',
    
    -- Score Data
    name LowCardinality(String),
    value Float64,
    data_type LowCardinality(String),
    string_value String DEFAULT '',
    
    -- Evaluation Metadata
    source LowCardinality(String),
    comment String CODEC(ZSTD(1)),
    author_user_id String DEFAULT '',
    config_id String DEFAULT '',
    eval_execution_trace_id String DEFAULT '',
    queue_id String DEFAULT '',
    environment LowCardinality(String) DEFAULT 'default',
    
    -- Additional Context
    service_name LowCardinality(String),
    agent_name LowCardinality(String),
    user_id String,
    metadata String CODEC(ZSTD(1)),
    
    -- Event Sourcing
    is_deleted UInt8,
    
    -- Indexes for fast lookups
    INDEX idx_id id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_span (tenant_id, trace_id, span_id) 
        TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_session (tenant_id, session_id) 
        TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_dataset_run (tenant_id, dataset_run_id) 
        TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tenant_id, project_id, toDate(timestamp), name, id)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Materialized view for trace-level evaluation summaries
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trace_score_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, project_id, trace_id, name, day)
AS SELECT
    tenant_id,
    project_id,
    trace_id,
    name,
    avgState(value) as avg_value,
    minState(value) as min_value,
    maxState(value) as max_value,
    countState() as count,
    toStartOfDay(timestamp) as day
FROM evaluation_scores
WHERE data_type = 'NUMERIC' AND is_deleted = 0
GROUP BY tenant_id, project_id, trace_id, name, day;

-- Materialized view for session-level evaluation summaries
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_session_score_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, project_id, session_id, name, day)
AS SELECT
    tenant_id,
    project_id,
    session_id,
    name,
    avgState(value) as avg_value,
    minState(value) as min_value,
    maxState(value) as max_value,
    countState() as count,
    toStartOfDay(timestamp) as day
FROM evaluation_scores
WHERE data_type = 'NUMERIC' AND is_deleted = 0 AND session_id != ''
GROUP BY tenant_id, project_id, session_id, name, day;

