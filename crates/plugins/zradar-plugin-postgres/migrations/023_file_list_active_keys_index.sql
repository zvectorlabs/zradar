-- Index for StorageUsageDailyJob::list_active_keys().
--
-- The query is:
--   SELECT DISTINCT tenant_id, project_id, signal_type
--   FROM file_list
--   WHERE deleted = false AND created_at < $1
--
-- The existing idx_file_list_active_size covers (tenant_id, project_id, signal_type)
-- WHERE deleted = false but does not include created_at, forcing a heap fetch to
-- evaluate the created_at filter. This index puts created_at first so Postgres can
-- do a range scan and read all projected columns from the index alone (index-only scan).
CREATE INDEX IF NOT EXISTS idx_file_list_active_keys
    ON file_list (created_at, tenant_id, project_id, signal_type)
    WHERE deleted = false;
