-- Mirrors migrations/040_task_fault_class.sql (SQLite). See there for the full
-- reasoning; the index is split into 051 per the 022/023 convention.
--
-- Fault attribution on the task record: what actually broke, quotable evidence
-- for it, and how much the verdict should be trusted.
--
-- `status = 'failed'` says the engine gave up. It has never been able to say
-- what broke, so every consumer of a failure — the retry decision, the
-- overview, the person paged at 3am — re-derived the cause from logs that are
-- already rotated. On a GPU fleet that gap is the cost model: an XID 79 and a
-- NaN loss are both 'failed', and retrying the second like the first burns
-- another N GPU-hours to reach the same answer.
--
-- Columns rather than a side table, for the same reason 044 put triage on the
-- run: there is exactly one verdict per attempt, and the hot read — "why did
-- this fail, and should the next attempt happen" — runs on the retry path,
-- where a join per completion is a cost with nothing to show for it.
--
-- NULL = unclassified, the pre-migration state of every existing row and the
-- honest state for a failure nothing matched. Deliberately distinguishable
-- from the string 'unknown' = "we looked and could not tell"; the two need
-- different follow-up.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fault_class      TEXT;
-- The line that produced the verdict, capped by the writer
-- (fault::EVIDENCE_MAX). A class with no quotable evidence is an assertion,
-- and nobody drains a node on an assertion.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fault_detail     TEXT;
-- 'low' | 'medium' | 'high'. Beside the class rather than folded into it: a
-- low-confidence gpu-xid and a high-confidence one warrant different actions.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fault_confidence TEXT;
