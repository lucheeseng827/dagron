-- Workflow lifecycle: version history, and a state that is not deletion.
--
-- Two holes this closes.
--
-- 1. `PUT /api/workflows/{id}` overwrote `spec` in place. There was no version
--    column and no history table, so the previous definition was simply gone —
--    you could not see what a workflow looked like before an edit, let alone
--    put it back. Past *runs* were safe (task_runs snapshots its TaskSpec at
--    dispatch), but the definition's own history was not.
--
-- 2. The only way to stop a workflow was DELETE, which is worse than it looks:
--    `schedules.workflow_id` is ON DELETE CASCADE, so deleting a workflow
--    silently takes its cron schedules with it, with no warning and no undo.
--    "Stop running this for a while" had to be spelled "destroy it".

-- Append-only. A row is written on create and on every update, so the table is
-- the history and `workflows.spec` is just the current head — kept there so the
-- hot read (the scheduler's JOIN) stays a single indexed row.
CREATE TABLE IF NOT EXISTS workflow_versions (
    id          TEXT PRIMARY KEY NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    -- 1-based, per workflow. Not a global sequence: "version 3" should mean the
    -- third edit of this workflow, not the third edit in the installation.
    version     BIGINT NOT NULL,
    name        TEXT NOT NULL,
    spec        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    -- Email rather than user id: this is meant to be readable long after the
    -- account may be gone.
    created_by  TEXT,
    UNIQUE (workflow_id, version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_versions_wf
    ON workflow_versions(workflow_id, version DESC);

-- Current head, so a reader knows which version the live spec corresponds to
-- without counting rows.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- active   — normal.
-- paused   — schedules do not fire and manual runs are refused. Reversible,
--            and crucially it does not touch the schedules themselves.
-- retired  — soft delete: hidden from the default listing, refuses to run, but
--            the definition and its history survive for reference.
--
-- Defaulting to 'active' keeps every existing workflow exactly as it was.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'active';

-- The scheduler filters on this every tick, alongside the schedules join.
CREATE INDEX IF NOT EXISTS idx_workflows_state ON workflows(state);
