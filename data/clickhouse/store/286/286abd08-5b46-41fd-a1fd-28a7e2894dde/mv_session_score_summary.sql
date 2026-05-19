ATTACH MATERIALIZED VIEW _ UUID 'f61f6878-f999-41c0-a00e-0e6c2c8782e3' TO INNER UUID 'efed1525-278b-4246-987a-8fd1f57861c1'
(
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `session_id` String,
    `name` LowCardinality(String),
    `avg_value` AggregateFunction(avg, Float64),
    `min_value` AggregateFunction(min, Float64),
    `max_value` AggregateFunction(max, Float64),
    `count` AggregateFunction(count),
    `day` DateTime('UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, project_id, session_id, name, day)
SETTINGS index_granularity = 8192 AS
SELECT
    tenant_id,
    project_id,
    session_id,
    name,
    avgState(value) AS avg_value,
    minState(value) AS min_value,
    maxState(value) AS max_value,
    countState() AS count,
    toStartOfDay(timestamp) AS day
FROM telemetry.evaluation_scores
WHERE (data_type = 'NUMERIC') AND (is_deleted = 0) AND (session_id != '')
GROUP BY
    tenant_id,
    project_id,
    session_id,
    name,
    day
