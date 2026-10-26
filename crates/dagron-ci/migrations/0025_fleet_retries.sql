-- Mirrors migrations_pg/0025_fleet_retries.sql: a fleet job that is to be
-- run again because its runner died. The comments there say what each is
-- for.
ALTER TABLE ci_fleet_jobs ADD COLUMN retry TEXT
    CHECK (retry IN ('due', 'asked', 'refused'));
ALTER TABLE ci_fleet_jobs ADD COLUMN retry_at TEXT;
ALTER TABLE ci_fleet_jobs ADD COLUMN retry_note TEXT;
CREATE INDEX ci_fleet_jobs_retry_due ON ci_fleet_jobs (repo_id) WHERE retry = 'due';
