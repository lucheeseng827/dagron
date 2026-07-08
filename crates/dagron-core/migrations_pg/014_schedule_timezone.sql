-- Per-schedule IANA timezone for DST-safe cron (spec fast-win #5).
-- Mirrors migrations/011_schedule_timezone.sql (SQLite). 'UTC' default keeps
-- every existing row behaving exactly as before; the engine schedule loop and
-- dagron-api interpret `cron_expr` in this zone when advancing `next_fire_at`.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC';
