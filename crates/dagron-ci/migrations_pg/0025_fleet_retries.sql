-- Fleet mode: a job that failed because its runner died, and not
-- because of what it ran, is run again once, through GitHub's API.
--   due      its runner died, and GitHub is to be asked to run it again
--   asked    GitHub took the ask: a new attempt of the job is queued
--   refused  it was not run again, and `retry_note` says why
-- NULL for a job that is no such job, or not yet seen to be.
ALTER TABLE ci_fleet_jobs ADD COLUMN retry TEXT
    CHECK (retry IN ('due', 'asked', 'refused'));
-- When a job that is due is next asked about: GitHub runs a job again only
-- once its workflow run is over, and the job's is not, while another of its
-- jobs still runs.
ALTER TABLE ci_fleet_jobs ADD COLUMN retry_at TIMESTAMPTZ;
ALTER TABLE ci_fleet_jobs ADD COLUMN retry_note TEXT;
CREATE INDEX ci_fleet_jobs_retry_due ON ci_fleet_jobs (repo_id) WHERE retry = 'due';
