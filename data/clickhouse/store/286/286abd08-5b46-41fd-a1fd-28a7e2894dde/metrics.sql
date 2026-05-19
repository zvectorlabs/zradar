ATTACH TABLE _ UUID 'f4824a68-0317-4e94-a97c-957f9e784b4b'
(
    `metric_name` LowCardinality(String),
    `metric_type` Enum8('COUNTER' = 1, 'GAUGE' = 2, 'HISTOGRAM' = 3, 'SUMMARY' = 4),
    `timestamp` DateTime64(9, 'UTC'),
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `value` Float64,
    `count` UInt64,
    `sum` Float64,
    `min` Float64,
    `max` Float64,
    `service_name` LowCardinality(String),
    `agent_name` LowCardinality(String),
    `user_id` String,
    `session_id` String,
    `labels` String CODEC(LZ4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, project_id, metric_name, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192
