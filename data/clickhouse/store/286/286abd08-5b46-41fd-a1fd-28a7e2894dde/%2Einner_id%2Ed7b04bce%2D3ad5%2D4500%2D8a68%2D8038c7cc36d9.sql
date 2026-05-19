ATTACH TABLE _ UUID 'ef6f154f-aebf-4538-b7fc-046c9fe2f202'
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
SETTINGS index_granularity = 8192
