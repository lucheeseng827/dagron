-- Mirrors migrations_pg/0027_fleet_run_ends.sql: how a fleet runner's run
-- was seen to end, and a GitLab project's look asked for while one was
-- going. The comments there say what each is for.
ALTER TABLE ci_fleet_runners ADD COLUMN run_end TEXT;
ALTER TABLE ci_repos ADD COLUMN fleet_poked_at TEXT;
