-- Task dispatch priority (spec fast-win #25 — Airflow `priority_weight` /
-- Argo Kueue analog). Among the tasks that are `ready` at the same moment a
-- scheduler claims higher `priority` first (claim_ready ORDER BY priority DESC,
-- scheduled_at), so a latency-sensitive branch jumps a deep backlog of
-- low-priority work. Persisted per row so a retry / lease recovery keeps its
-- place in line. DEFAULT 0 leaves every existing task — and every task that
-- names no priority — exactly as before: a pure tiebreak within the
-- scheduled_at order, so an unprioritized deployment is unchanged.
ALTER TABLE task_runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- Priority-first analog of the class ready index (019): the claim scan reads
-- rows already in claim order (highest priority, then oldest) without a sort,
-- staying tiny under a deep ready backlog.
CREATE INDEX IF NOT EXISTS idx_task_runs_ready_priority
    ON task_runs (priority DESC, scheduled_at)
    WHERE status = 'ready';
