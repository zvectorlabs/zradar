ATTACH TABLE _ UUID '9dc82b33-01df-4af9-ba9c-492bf12ec080'
(
    `tenant_id` LowCardinality(String),
    `project_id` LowCardinality(String),
    `date` Date,
    `llm_model` LowCardinality(String),
    `span_count` UInt64,
    `total_prompt_tokens` UInt64,
    `total_completion_tokens` UInt64,
    `total_tokens` UInt64,
    `total_cost_usd` Float64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (tenant_id, project_id, date, llm_model)
SETTINGS index_granularity = 8192
