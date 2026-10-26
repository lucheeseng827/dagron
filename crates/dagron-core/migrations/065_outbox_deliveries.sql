-- The outbox, delivered per sink. See
-- migrations_pg/088_outbox_deliveries.sql.
CREATE TABLE IF NOT EXISTS outbox_sinks (
    sink          TEXT PRIMARY KEY,
    since         TEXT NOT NULL,
    cursor        TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_deliveries (
    outbox_id       TEXT NOT NULL,
    sink            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error      TEXT,
    delivered_at    TEXT,
    PRIMARY KEY (outbox_id, sink)
);
CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_due
    ON outbox_deliveries (sink, status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_event_outbox_created
    ON event_outbox (created_at);

CREATE TABLE IF NOT EXISTS outbox_epoch (
    per_sink_since TEXT NOT NULL
);
INSERT INTO outbox_epoch (per_sink_since)
SELECT strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
WHERE NOT EXISTS (SELECT 1 FROM outbox_epoch);
