-- A run whose start is still to be announced: the `pending` commit status its
-- `notify.git` block asks for.
--
-- Set when a run whose spec declares `notify.git` is created, by whichever
-- process creates it. dagron-api, which submits most runs, holds no forge
-- credentials; the engine does. The next reconcile tick on an engine replica
-- with a forge token claims the stamp (`claim_pending_starts`) and posts
-- `pending`, so a pull request shows the check as running from the moment the
-- run exists. A run that ends first has the stamp cleared with no post: its
-- terminal status is the one that matters then.
--
-- NULL on every other row, existing ones included. The partial index keeps the
-- claim a probe of a near-empty set.
ALTER TABLE workflow_runs ADD COLUMN start_pending_at TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_start_pending
    ON workflow_runs (start_pending_at) WHERE start_pending_at IS NOT NULL;
