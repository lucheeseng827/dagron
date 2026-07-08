-- Per-schedule IANA timezone for DST-safe cron (spec fast-win #5).
-- 'UTC' preserves the historical behavior for every existing row, so this is a
-- backward-compatible additive change. The engine's schedule loop and the
-- dagron-api schedule drawer interpret `cron_expr` in this zone when computing
-- and advancing `next_fire_at`.
ALTER TABLE schedules ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';
