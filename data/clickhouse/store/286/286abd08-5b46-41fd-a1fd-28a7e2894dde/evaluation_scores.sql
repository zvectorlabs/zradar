ATTACH TABLE _ UUID '28919008-9f9c-4c95-8d8b-723f37ff719b'
(
    `id` String,
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `timestamp` DateTime64(3, 'UTC'),
    `created_at` DateTime64(3, 'UTC'),
    `updated_at` DateTime64(3, 'UTC'),
    `event_ts` DateTime64(3, 'UTC'),
    `trace_id` String,
    `observation_id` String DEFAULT '',
    `session_id` String DEFAULT '',
    `dataset_run_id` String DEFAULT '',
    `name` LowCardinality(String),
    `value` Float64,
    `data_type` LowCardinality(String),
    `string_value` String DEFAULT '',
    `source` LowCardinality(String),
    `comment` String CODEC(ZSTD(1)),
    `author_user_id` String DEFAULT '',
    `config_id` String DEFAULT '',
    `eval_execution_trace_id` String DEFAULT '',
    `queue_id` String DEFAULT '',
    `environment` LowCardinality(String) DEFAULT 'default',
    `service_name` LowCardinality(String),
    `agent_name` LowCardinality(String),
    `user_id` String,
    `metadata` String CODEC(LZ4),
    `is_deleted` UInt8,
    INDEX idx_id id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_obs (tenant_id, trace_id, observation_id) TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_session (tenant_id, session_id) TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_dataset_run (tenant_id, dataset_run_id) TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tenant_id, project_id, toDate(timestamp), name, id)
TTL toDateTime(timestamp) + toIntervalDay(90)
SETTINGS index_granularity = 8192
