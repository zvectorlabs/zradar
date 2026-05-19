ATTACH MATERIALIZED VIEW _ UUID 'e91a495a-0fea-48f2-9c09-3df768082277' TO INNER UUID '9dc82b33-01df-4af9-ba9c-492bf12ec080'
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
SETTINGS index_granularity = 8192 AS
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
FROM telemetry.spans
WHERE llm_model != ''
GROUP BY
    tenant_id,
    project_id,
    date,
    llm_model
