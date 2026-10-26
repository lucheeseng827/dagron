-- Mirrors migrations_pg/0006_dispatch_pipeline.sql: the one pipeline a
-- dispatch runs. The comments there say what it is for.
ALTER TABLE ci_events ADD COLUMN pipeline TEXT;
