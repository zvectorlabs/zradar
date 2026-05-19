ATTACH MATERIALIZED VIEW _ UUID '3bba0041-1b17-4ae9-b1b8-0ea46f09235f' TO INNER UUID '5d242a2e-9c8d-4fdf-a1cd-a18ca5dbc760'
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
SETTINGS index_granularity = 8192 AS
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
FROM telemetry.spans
WHERE (span_kind = 'INTERNAL') AND (agent_name != '')
GROUP BY
    tenant_id,
    project_id,
    agent_name,
    date
