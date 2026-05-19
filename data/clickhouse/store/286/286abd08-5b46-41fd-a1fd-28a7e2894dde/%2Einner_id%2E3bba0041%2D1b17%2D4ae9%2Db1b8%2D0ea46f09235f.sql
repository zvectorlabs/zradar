ATTACH TABLE _ UUID '5d242a2e-9c8d-4fdf-a1cd-a18ca5dbc760'
(
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `agent_name` LowCardinality(String),
    `date` Date,
    `invocation_count` UInt64,
    `avg_duration_ns` AggregateFunction(avg, UInt64),
    `p50_duration_ns` AggregateFunction(quantile(0.5), UInt64),
    `p95_duration_ns` AggregateFunction(quantile(0.95), UInt64),
    `p99_duration_ns` AggregateFunction(quantile(0.99), UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (tenant_id, project_id, agent_name, date)
SETTINGS index_granularity = 8192
