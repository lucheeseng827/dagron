-- Fleet mode: a label keeps runners warm for a repository that has
-- had a job of it lately. The newest job of each of a repository's labels is
-- read at each look.
CREATE INDEX ci_fleet_jobs_newest ON ci_fleet_jobs (repo_id, class, queued_at);
