ATTACH MATERIALIZED VIEW _ UUID 'd7b04bce-3ad5-4500-8a68-8038c7cc36d9' TO INNER UUID 'ef6f154f-aebf-4538-b7fc-046c9fe2f202'
(
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `trace_id` String,
    `name` LowCardinality(String),
    `avg_value` AggregateFunction(avg, Float64),
    `min_value` AggregateFunction(min, Float64),
    `max_value` AggregateFunction(max, Float64),
    `count` AggregateFunction(count),
    `day` DateTime('UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, project_id, trace_id, name, day)
SETTINGS index_granularity = 8192 AS
SELECT
    tenant_id,
    project_id,
    trace_id,
    name,
    avgState(value) AS avg_value,
    minState(value) AS min_value,
    maxState(value) AS max_value,
    countState() AS count,
    toStartOfDay(timestamp) AS day
FROM telemetry.evaluation_scores
WHERE (data_type = 'NUMERIC') AND (is_deleted = 0)
GROUP BY
    tenant_id,
    project_id,
    trace_id,
    name,
    day
