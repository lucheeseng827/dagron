-- The run API: `POST /ci/v1/runs` queues a dispatch, an event a caller asks
-- for rather than one a forge announced.
--
-- A dispatch's inputs, a JSON object of strings, become its run's parameters
-- beside the ci_* ones. NULL for every other event.
ALTER TABLE ci_events ADD COLUMN inputs TEXT;

-- Idempotency-Key dedupe for the run API: one row per caller and key,
-- written in the same transaction as the event it produced. A repeat with the
-- same request finds its event; the same key with another request is refused.
-- `principal` is the caller's subject in dagron-api, so two callers' keys
-- never meet. `fingerprint` is the SHA-256 of the request as it was sent.
CREATE TABLE ci_api_requests (
    principal       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint     TEXT NOT NULL,
    event_id        TEXT NOT NULL REFERENCES ci_events (id) ON DELETE CASCADE,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (principal, idempotency_key)
);

-- `GET /ci/v1/runs/{id}` finds the engine run an event submitted.
CREATE INDEX ci_runs_event ON ci_runs (event_id);
