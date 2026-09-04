-- Fault attribution on the task record (mirrors migrations_pg/050_task_fault_class.sql).
--
-- `status = 'failed'` says the engine gave up. It has never been able to say
-- what broke, so every consumer of a failure — the retry decision, the
-- overview, the person paged at 3am — has had to re-derive the cause by
-- reading logs that are already rotated. On a GPU fleet that gap is the cost
-- model: an XID 79 and a NaN loss are both 'failed', and retrying the second
-- like the first burns another N GPU-hours to reach the same answer.
--
-- Three columns, not a side table, for the same reason 037 put triage on the
-- run: there is exactly one verdict per attempt, and the hot read — "why did
-- this fail, and should the next attempt happen" — runs on the retry path,
-- where a join per completion is a cost with nothing to show for it.
--
-- NULL = unclassified, which is the pre-migration behaviour for every existing
-- row and the honest state for a failure nothing matched. It is deliberately
-- distinguishable from the string 'unknown', which means "we looked and could
-- not tell" — the two need different follow-up.
ALTER TABLE task_runs ADD COLUMN fault_class TEXT;
-- The line that produced the verdict, capped by the writer (fault::EVIDENCE_MAX).
-- A class with no quotable evidence is an assertion, and nobody drains a node
-- on an assertion.
ALTER TABLE task_runs ADD COLUMN fault_detail TEXT;
-- 'low' | 'medium' | 'high'. Kept beside the class rather than folded into it
-- because a low-confidence gpu-xid and a high-confidence one warrant different
-- actions, and collapsing them loses the only thing the operator wants.
ALTER TABLE task_runs ADD COLUMN fault_confidence TEXT;

-- The fleet read is "what did this class cost us lately", which scans failed
-- rows by class over a time range. Partial on the classified rows only: the
-- unclassified ones are the majority and this index never has to look at them.
CREATE INDEX IF NOT EXISTS idx_task_runs_fault_class
    ON task_runs(fault_class, finished_at)
    WHERE fault_class IS NOT NULL;
