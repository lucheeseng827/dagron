-- Fleet mode, the rest of its first version: a fork's job told
-- apart, a job GitHub never said the end of asked about, an organization's
-- runners, and a class's runners counted against its quota.

-- Whether the workflow run a job is of has its head in the repository
-- (`trusted`) or in another, a fork's pull request (`untrusted`), as GitHub
-- says of the run. NULL until asked, which is done only for a job whose
-- class tells the two apart.
ALTER TABLE ci_fleet_jobs ADD COLUMN trust TEXT
    CHECK (trust IN ('trusted', 'untrusted'));
-- When GitHub was last asked how the job stands: a job that waits longer
-- than a delivery takes is asked about, in case its delivery never came.
ALTER TABLE ci_fleet_jobs ADD COLUMN asked_at TIMESTAMPTZ;
CREATE INDEX ci_fleet_jobs_open ON ci_fleet_jobs (repo_id) WHERE state <> 'done';

-- The organization a runner is registered for, where it is an
-- organization's: it is removed there. NULL for a repository's runner.
ALTER TABLE ci_fleet_runners ADD COLUMN org TEXT;
-- What the run that hosts it was submitted as: `untrusted` for a runner
-- started for a fork's job, which the engine holds to its floor.
ALTER TABLE ci_fleet_runners ADD COLUMN trust TEXT NOT NULL DEFAULT 'trusted'
    CHECK (trust IN ('trusted', 'untrusted'));
-- A class's runners out, for its quota, whatever their repository.
CREATE INDEX ci_fleet_runners_out ON ci_fleet_runners (class) WHERE state <> 'ended';
