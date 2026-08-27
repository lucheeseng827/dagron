-- Idempotency keys on submit (G-AG2): make `POST /api/runs` safe to retry.
--
-- Agents retry. Today a dropped response means the caller cannot tell a
-- duplicated run from a lost one, and neither guess is safe: re-submitting may
-- run a pipeline twice, and not re-submitting may run it zero times. This is
-- the difference between an API an agent calls and an API an agent can call in
-- a loop.
--
-- A side table rather than a column on workflow_runs, for one reason: the row
-- must exist *before* the run does. The claim is inserted first and resolved to
-- a run_id afterwards, which is what makes two concurrent identical submits
-- resolve to one run instead of racing between a lookup and an insert. A column
-- on a row that does not exist yet cannot do that.
--
-- Retention is deliberately not its own sweep. The FK cascades from the run, so
-- a key lives exactly as long as the run it names: when the v6 retention GC
-- archives and purges a run, its key goes with it and becomes reusable. A
-- separate TTL would be a second policy to keep in step with the first, and the
-- interesting failure — a key outliving its run, replaying to a run_id that no
-- longer resolves — is the one it would introduce.
CREATE TABLE IF NOT EXISTS run_idempotency (
    -- The authenticated principal (SessionClaims.sub) that made the claim. A key
    -- is scoped to the caller that chose it: two principals sharing one engine
    -- can reuse the same key string without one receiving the other's run_id (or
    -- a 409 for a body it never sent). A key is a caller's private handle on its
    -- own retry, not a name every caller on the database competes for.
    principal       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    -- NULL while the submit holding this claim is still in flight. A reader
    -- that finds NULL knows the original is mid-submit, which is a different
    -- answer from "no such key" and must not be reported as one.
    run_id          TEXT REFERENCES workflow_runs(id) ON DELETE CASCADE,
    -- SHA-256 over the submitted request. A key replayed with a *different*
    -- body is a client bug, and the dangerous response is to serve the first
    -- run as though it were the second — so that case is rejected, not served.
    fingerprint     TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    -- Uniqueness is per principal, not global: the claim a key names belongs to
    -- the caller that made it.
    PRIMARY KEY (principal, idempotency_key)
);

-- Reclaiming a stale in-flight claim (submitter died between claiming the key
-- and creating the run) scans for run_id IS NULL. Partial, because a resolved
-- claim is never a candidate.
CREATE INDEX IF NOT EXISTS idx_run_idempotency_inflight
    ON run_idempotency(created_at)
    WHERE run_id IS NULL;
