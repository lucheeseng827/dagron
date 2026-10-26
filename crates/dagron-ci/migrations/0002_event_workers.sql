-- Mirrors migrations_pg/0002_event_workers.sql: event workers, what a
-- repository's events run, and the runs dagron-ci submitted. Timestamps are
-- RFC 3339 text; the comments there say what each column is for.
ALTER TABLE ci_events ADD COLUMN claimed_by       TEXT;
ALTER TABLE ci_events ADD COLUMN claim_expires_at TEXT;
ALTER TABLE ci_events ADD COLUMN attempts         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ci_events ADD COLUMN next_attempt_at  TEXT;
ALTER TABLE ci_events ADD COLUMN reason           TEXT;
CREATE INDEX ci_events_claimed ON ci_events (claim_expires_at) WHERE state = 'claimed';

ALTER TABLE ci_repos ADD COLUMN workflow TEXT;
ALTER TABLE ci_repos ADD COLUMN cell     TEXT NOT NULL DEFAULT 'default';

CREATE TABLE ci_runs (
    run_id          TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL,
    repo_id         TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    cell            TEXT NOT NULL,
    pipeline        TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    frontend        TEXT NOT NULL CHECK (frontend IN ('native', 'github', 'gitlab')),
    status          TEXT NOT NULL DEFAULT 'submitted',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX ci_runs_open ON ci_runs (created_at)
    WHERE status NOT IN ('succeeded', 'failed', 'cancelled');
