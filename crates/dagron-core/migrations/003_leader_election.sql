-- Workflow scheduler v5 — leadership lease (SQLite).
--
-- The same lease pattern that gives task crash-recovery its correctness (a row
-- with an expiry that any process can reclaim) generalizes to a cluster-wide
-- singleton. Cron firing and retention GC must run on exactly one scheduler at a
-- time; rather than a heartbeat table + coordinator, one `leader_election` row
-- per role IS the lock. A scheduler holds the role only while its lease is
-- unexpired; if it dies, the lease lapses and the next renewing peer takes over.
--
-- This keeps the singleton backend-agnostic (the Postgres mirror is identical),
-- staying true to the "the row is the truth" inversion. On Postgres a native
-- `pg_advisory_lock` is an alternative, but the lease row keeps the db API and
-- semantics byte-identical across both backends.

CREATE TABLE IF NOT EXISTS leader_election (
    role             TEXT PRIMARY KEY NOT NULL,
    holder           TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL
);
