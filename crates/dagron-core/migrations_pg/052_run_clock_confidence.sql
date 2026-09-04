-- Mirrors migrations/041_run_clock_confidence.sql (SQLite). See there for the
-- full reasoning.
--
-- Clock confidence on the run record: whether the engine's wall clock was
-- trustworthy when the run was written. A unit that boots with no network and
-- no RTC battery keeps scheduling — recovery is never gated on the clock — but
-- every run it creates says which clock it was stamped under, so an auditor
-- can tell evidence from a guess.
--
-- 'synced' | 'drifted' | 'unknown'. NULL = a row written before this
-- migration, or by a writer that bypasses the datastore facade; the engine
-- itself always stamps a value ('unknown' when it has assessed nothing).
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS clock_confidence TEXT;
-- Signed wall-vs-monotonic offset (ms) behind a 'drifted' verdict; NULL when
-- there is no measurement to report.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS clock_offset_ms  BIGINT;
-- 'sync-file' | 'step' | 'behind-datastore' — what produced the verdict.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS clock_source     TEXT;
