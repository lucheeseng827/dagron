-- Transactional outbox: run-lifecycle events written in the SAME transaction as
-- the run's finalization (see db::reap_completed_runs), so a committed state
-- change always has its event and a rolled-back one never emits a ghost. A
-- delivery worker (Enterprise: ee/dagron-events) drains pending rows to webhooks /
-- queues / downstream workflows; the OSS engine just records them durably.
CREATE TABLE IF NOT EXISTS event_outbox (
    id              TEXT PRIMARY KEY NOT NULL,
    run_id          TEXT NOT NULL,
    event_type      TEXT NOT NULL,            -- e.g. 'run.completed'
    payload         TEXT NOT NULL,            -- JSON event body
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | dead
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,            -- rfc3339; when eligible for delivery
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    delivered_at    TEXT
);

-- The drainer claims due pending rows with FOR UPDATE SKIP LOCKED ordered by
-- next_attempt_at — same coordination-free pattern as task claiming.
CREATE INDEX IF NOT EXISTS idx_event_outbox_due
    ON event_outbox (status, next_attempt_at);
