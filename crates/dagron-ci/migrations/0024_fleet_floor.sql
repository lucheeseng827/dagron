-- Mirrors migrations_pg/0024_fleet_floor.sql: a fleet job's trust and when
-- GitHub was last asked of it, and a runner's organization and trust. The
-- comments there say what each is for.
ALTER TABLE ci_fleet_jobs ADD COLUMN trust TEXT
    CHECK (trust IN ('trusted', 'untrusted'));
ALTER TABLE ci_fleet_jobs ADD COLUMN asked_at TEXT;
CREATE INDEX ci_fleet_jobs_open ON ci_fleet_jobs (repo_id) WHERE state <> 'done';

ALTER TABLE ci_fleet_runners ADD COLUMN org TEXT;
ALTER TABLE ci_fleet_runners ADD COLUMN trust TEXT NOT NULL DEFAULT 'trusted'
    CHECK (trust IN ('trusted', 'untrusted'));
CREATE INDEX ci_fleet_runners_out ON ci_fleet_runners (class) WHERE state <> 'ended';
