CREATE INDEX IF NOT EXISTS idx_file_list_query
    ON file_list (tenant_id, project_id, signal_type, deleted, max_ts, min_ts);

CREATE INDEX IF NOT EXISTS idx_file_list_mover
    ON file_list (location, deleted, created_at DESC)
    WHERE location = 'local';

CREATE INDEX IF NOT EXISTS idx_file_list_compactor
    ON file_list (tenant_id, project_id, signal_type, date, deleted);
