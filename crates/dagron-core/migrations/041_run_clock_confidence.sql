-- Clock confidence on the run record (mirrors migrations_pg/052_run_clock_confidence.sql).
--
-- Every timestamp dagron writes comes from the engine process's own wall
-- clock. In a datacentre with NTP that is a benign assumption; on a unit that
-- boots with no network and no RTC battery the clock can be wrong by years,
-- and a regulator reading `created_at` has no way to know. These three
-- columns are the engine saying, per run, whether its clock was trustworthy
-- when the record was written — so a record stamped under an unsynced clock
-- says so instead of passing for evidence.
--
-- Columns rather than a side table, for the same reason 037 put triage on the
-- run: exactly one verdict per run, read together with the run, never joined.
--
-- 'synced' | 'drifted' | 'unknown'. NULL = a row written before this
-- migration, or by a writer that bypasses the datastore facade. The engine
-- itself always stamps a value — 'unknown' when it has assessed nothing,
-- which is an honest answer and deliberately distinguishable from 'drifted'
-- (something looked and found the clock wrong).
ALTER TABLE workflow_runs ADD COLUMN clock_confidence TEXT;
-- Measured wall-vs-monotonic offset in milliseconds behind a 'drifted'
-- verdict: the step the detector caught (signed — negative when the clock was
-- set back), or how far behind the newest run on disk the clock read at boot.
-- NULL when there is no measurement to report.
ALTER TABLE workflow_runs ADD COLUMN clock_offset_ms INTEGER;
-- What produced the verdict: 'sync-file' (positive evidence from the host's
-- time daemon, DAGRON_CLOCK_SYNC_FILE), 'step' (the wall clock jumped against
-- the monotonic clock), 'behind-datastore' (boot plausibility: now < the
-- newest run already on disk).
ALTER TABLE workflow_runs ADD COLUMN clock_source TEXT;
