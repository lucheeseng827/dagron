-- Mirrors migrations_pg/0004_run_api.sql: a dispatch's inputs, and the run
-- API's Idempotency-Key dedupe. The comments there say what each is for.
ALTER TABLE ci_events ADD COLUMN inputs TEXT;

CREATE TABLE ci_api_requests (
    principal       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint     TEXT NOT NULL,
    event_id        TEXT NOT NULL REFERENCES ci_events (id) ON DELETE CASCADE,
    received_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (principal, idempotency_key)
);

CREATE INDEX ci_runs_event ON ci_runs (event_id);
