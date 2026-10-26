-- Who writes under a commit's status context (a job's check, as
-- `dagron-ci/<pipeline>/<job> (<event>)`), so that a passing "skipped" one
-- never stands over what a run of the job came to.
--
-- A run's reporter owns a context (`run_id`) before it first posts under
-- it. An event that says a job skipped (left out by its `if:` or `when:`,
-- or its pipeline by `paths:`) posts only under a lease (`lease_holder`,
-- `lease_until`), taken where no run owns the context and no other lease
-- is in force, and given back once the post is answered. A run waits out a
-- lease before it owns the context, so its post is the later; and once a
-- run owns it, no lease is had. A lease outlasts the longest post, and one
-- not given back lapses.
--
-- A row is dated by its last write (`written_at`): its owning, by any run,
-- or a lease. It goes once records of that day are dropped
-- (CI_RECORD_DAYS), which waits on the day's runs: a context a later
-- event's run owns is kept with that run's day.
CREATE TABLE ci_contexts (
    repo_id      TEXT NOT NULL,
    sha          TEXT NOT NULL,
    context      TEXT NOT NULL,
    run_id       TEXT,
    lease_holder TEXT,
    lease_until  TIMESTAMPTZ,
    written_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, sha, context)
);
CREATE INDEX ci_contexts_written ON ci_contexts (written_at);
