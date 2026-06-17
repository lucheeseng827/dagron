-- Workflow scheduler v5 — leadership lease (Postgres).
--
-- Mirrors migrations/003_leader_election.sql one-for-one. One `leader_election`
-- row per role is the cluster-wide singleton lock for cron firing and retention
-- GC: a scheduler holds the role only while its lease is unexpired, and a dead
-- holder's lease simply lapses for the next renewing peer to take over — the same
-- lease-is-the-truth pattern used for task crash recovery.

CREATE TABLE IF NOT EXISTS leader_election (
    role             TEXT PRIMARY KEY NOT NULL,
    holder           TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL
);
