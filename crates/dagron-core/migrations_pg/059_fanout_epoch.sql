-- Mirrors migrations/046_fanout_epoch.sql (SQLite). See there: a fan-out
-- barrier's instances are its dependencies, so no reset path's downstream cone
-- reaches them, and a re-run would otherwise join against the previous
-- attempt's rows. The barrier's `version` at creation is stamped here so the
-- sweep can tell a current instance from a stale one in one place.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fanout_epoch BIGINT NOT NULL DEFAULT 0;
