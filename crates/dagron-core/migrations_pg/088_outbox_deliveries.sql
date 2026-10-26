-- The outbox, delivered per sink. `event_outbox` kept one
-- delivery state a row, so the first consumer to claim an event decided that
-- no other ever saw it. Each sink (the webhook, the fleet uplink, the engine's
-- own end-of-run work) now keeps its own: a row here for each event it was
-- handed, by the event's id and the sink's name, leased and retried as the
-- outbox's own row was.
--
-- `outbox_sinks`: each sink by name, `since` the first event it is handed and
-- `cursor` how far it has enrolled; a sink sees the events written after it
-- was first configured. `event_outbox.status` is now what that outbox
-- recorded before this migration: a row it says was delivered, dead or shed
-- is handed to no sink.
--
-- `outbox_epoch`: when this migration ran. A sink that took over from the
-- single-consumer outbox (the webhook, the uplink) is handed, as it is first
-- configured, the events written before then that the outbox still held
-- pending; every event written after then stays `pending` there, and is
-- handed only to the sinks configured by the time it was written.
CREATE TABLE IF NOT EXISTS outbox_sinks (
    sink          TEXT PRIMARY KEY,
    since         TEXT NOT NULL,
    cursor        TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_deliveries (
    outbox_id       TEXT NOT NULL,
    sink            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | dead | shed
    attempts        BIGINT NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error      TEXT,
    delivered_at    TEXT,
    PRIMARY KEY (outbox_id, sink)
);
CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_due
    ON outbox_deliveries (sink, status, next_attempt_at);

-- RFC 3339 in UTC, as the engine writes `created_at`, so that the two compare.
CREATE TABLE IF NOT EXISTS outbox_epoch (
    per_sink_since TEXT NOT NULL
);
INSERT INTO outbox_epoch (per_sink_since)
SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"')
WHERE NOT EXISTS (SELECT 1 FROM outbox_epoch);
