ALTER TABLE ingestion_events
    DROP CONSTRAINT IF EXISTS ingestion_events_file_id_fkey;

ALTER TABLE ingestion_events
    ADD CONSTRAINT ingestion_events_file_id_fkey
    FOREIGN KEY (file_id)
    REFERENCES file_list(id)
    ON DELETE SET NULL;
