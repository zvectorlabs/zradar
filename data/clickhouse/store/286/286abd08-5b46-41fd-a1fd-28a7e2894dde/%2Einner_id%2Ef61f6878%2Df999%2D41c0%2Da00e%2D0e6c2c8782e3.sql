ATTACH TABLE _ UUID 'efed1525-278b-4246-987a-8fd1f57861c1'
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
SETTINGS index_granularity = 8192
