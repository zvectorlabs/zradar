-- Store project-level LLM content capture policy.
ALTER TABLE project_settings
ADD COLUMN IF NOT EXISTS capture_llm_content_enabled BOOLEAN NOT NULL DEFAULT TRUE;
