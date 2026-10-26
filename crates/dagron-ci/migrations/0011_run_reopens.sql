-- Mirrors migrations_pg/0011_run_reopens.sql: when dagron-ci saw a run end,
-- which a sight of the run running again has to be later than. The comments
-- there say what it is for.
ALTER TABLE ci_runs ADD COLUMN ended_at TEXT;

-- How often the record re-opened. A job that failed, and fails again when its
-- run is run again, reads as it did: the count is what tells the reporter
-- that the result is a new one, to be posted again.
ALTER TABLE ci_runs ADD COLUMN reopens INTEGER NOT NULL DEFAULT 0;
