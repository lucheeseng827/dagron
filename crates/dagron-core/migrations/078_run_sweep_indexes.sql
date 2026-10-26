-- The run-level sweeps' indexes, led by `status`, and an index of the live
-- runs.
--
-- The rule is 076's: an index a sweep reads here is led by `status`, with the
-- column the sweep compares as its second key. Nothing writes sqlite_stat1
-- (init_pool runs no ANALYZE), and with no statistics SQLite costs an
-- equality on any index's first column at about ten rows. So on a build with
-- the `enterprise` feature, whose own migrations add an index of (status,
-- created_at), that index took six run sweeps from the partial indexes made
-- for them (010, 015, 053, 054, 055, 060), which were led by the column each
-- sweep compares, and read every running run, or for the completions sweep
-- every finished one, each tick. Led by `status`, each partial's loop uses
-- `status` and its second column, a strict superset of the terms that
-- index's loop uses, so it is the cheaper loop whichever index is the newer:
-- on a fresh enterprise file that index is made after this, and on an
-- upgraded one before. A second key of `id` would not do: its loop uses
-- `status` alone, ties with that index, and loses to whichever is the newer.
--
-- Each keeps its name and its WHERE word for word. Postgres keeps the old
-- definitions under the same names: its planner has statistics to take them
-- by, and 048 gives it (status, created_at) besides.
--
-- idx_workflow_runs_live serves the live tier of /metrics (status_counts_
-- unfinished, recent_runs of the unfinished) and dagron-api's health count.
-- SQLite lets a read use a partial index only if the read carries the
-- index's WHERE as written: this IN list must stay, word for word and in
-- order, models::LIVE_RUN_STATUSES as db::sql_list writes it, and
-- db::LIVE_RUNS_COUNT. The plan tests in db/sqlite.rs hold both to that
-- (live_reads_plan_on_the_live_index). It is not named
-- idx_workflow_runs_status_created_at: on a fresh enterprise file the
-- enterprise migration's IF NOT EXISTS would then skip its own index, which
-- twenty reads there use.
--
-- The plan tests (workflow_runs_sweeps_keep_their_partials,
-- run_sweeps_hold_against_a_status_index_of_either_age,
-- run_plans_that_must_not_move) hold these plans on both builds.
DROP INDEX IF EXISTS idx_workflow_runs_deadline;
CREATE INDEX idx_workflow_runs_deadline
    ON workflow_runs (status, deadline_at)
    WHERE deadline_at IS NOT NULL AND status = 'running';
DROP INDEX IF EXISTS idx_workflow_runs_alert_deadline;
CREATE INDEX idx_workflow_runs_alert_deadline
    ON workflow_runs (status, alert_deadline_at)
    WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running';
DROP INDEX IF EXISTS idx_workflow_runs_stopping;
CREATE INDEX idx_workflow_runs_stopping
    ON workflow_runs (status, stopping_as) WHERE stopping_as IS NOT NULL;
DROP INDEX IF EXISTS idx_workflow_runs_waiting;
CREATE INDEX idx_workflow_runs_waiting
    ON workflow_runs (status, queued_at, id)
    WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running';
DROP INDEX IF EXISTS idx_workflow_runs_start_pending;
CREATE INDEX idx_workflow_runs_start_pending
    ON workflow_runs (status, start_pending_at) WHERE start_pending_at IS NOT NULL;
DROP INDEX IF EXISTS idx_workflow_runs_completion_pending;
CREATE INDEX idx_workflow_runs_completion_pending
    ON workflow_runs (status, completion_pending_at) WHERE completion_pending_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_live
    ON workflow_runs (status, created_at) WHERE status IN ('pending', 'running');
