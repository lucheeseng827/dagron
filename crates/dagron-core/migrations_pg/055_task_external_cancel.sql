-- Mirrors migrations/043_task_external_cancel.sql (SQLite). See there for why
-- the "owes teardown" predicate needs no column of its own, and why it is
-- task-level rather than run-level.
--
-- No new index: the teardown sweep reuses the partial index from 054, whose
-- predicate (`external_handle IS NOT NULL`) and column (`next_poll_at`) are
-- already exactly what it filters and orders on.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_cancel_attempts BIGINT NOT NULL DEFAULT 0;
