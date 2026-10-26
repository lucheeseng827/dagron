-- Mirrors migrations_pg/0029_whole_reruns.sql: how many times the run was
-- rerun whole. The comments there say what it is for.
ALTER TABLE ci_runs ADD COLUMN whole_reruns INTEGER NOT NULL DEFAULT 0;
