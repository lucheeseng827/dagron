-- Exactly-once ingestion: the committed position of each streaming source
-- (byte offset, Kafka offset, replication LSN, …), written in the SAME
-- transaction that creates the run (or parks the dead letter) it accounts
-- for. A source that resumes from this row can never re-create a run for a
-- position it already committed — the run and the cursor move atomically.
CREATE TABLE IF NOT EXISTS source_offsets (
    source_name TEXT PRIMARY KEY,
    position    TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
