-- Mirrors migrations_pg/0005_pipelines.sql: the branch a pull request or merge
-- group merges into. The comments there say what it is for.
ALTER TABLE ci_events ADD COLUMN base_ref TEXT;
