-- Mirrors migrations_pg/0026_fleet_warm.sql: the newest job of each of a
-- repository's labels, read at each look.
CREATE INDEX ci_fleet_jobs_newest ON ci_fleet_jobs (repo_id, class, queued_at);
