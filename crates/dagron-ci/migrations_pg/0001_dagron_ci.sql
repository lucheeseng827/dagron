-- dagron-ci's own tables. They live in the schema CI_DATABASE_SCHEMA names
-- (`dagron_ci` by default), so they can share a Postgres with dagron-api
-- without sharing its migration history. Nothing in the engine reads them.

-- The repositories this dagron-ci serves. A webhook for a repository without a
-- row here is refused and writes nothing. Names are stored lowercased: GitHub
-- and GitLab match them without regard to case.
CREATE TABLE ci_repos (
    id         TEXT PRIMARY KEY,
    forge      TEXT NOT NULL
               CHECK (forge IN ('github', 'gitlab', 'gitea', 'forgejo', 'bitbucket', 'generic')),
    forge_host TEXT NOT NULL,
    full_name  TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (forge_host, full_name)
);

-- Delivery dedupe: one row per forge delivery id, written in the same
-- transaction as the event it produced. A redelivery finds its row and writes
-- nothing. Forges retry for hours, not weeks, so a sweep can keep this small.
CREATE TABLE ci_deliveries (
    forge_host  TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (forge_host, delivery_id)
);

-- The event queue. Every door (the webhook receivers now, the poll sweep and
-- the API later) writes one row here, and only this queue leads to a run.
--
-- `commit_key` is the commit-level natural key, `ci:{repo}:{kind}:{ref}:{sha}`:
-- a webhook and a later poll that see the same commit derive the same key, and
-- with it the same run idempotency key. `trust` is computed from the verified
-- event, never from a field an outsider controls. `payload` holds the raw body
-- when it is at most 64 KiB; larger ones are not kept.
CREATE TABLE ci_events (
    id          TEXT PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    repo_id     TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    source      TEXT NOT NULL CHECK (source IN ('webhook', 'poll', 'api', 'relay')),
    delivery_id TEXT,
    kind        TEXT NOT NULL,
    ref         TEXT NOT NULL,
    sha         TEXT NOT NULL,
    base_sha    TEXT,
    pr          BIGINT,
    actor       TEXT,
    trust       TEXT NOT NULL CHECK (trust IN ('trusted', 'untrusted', 'agent')),
    commit_key  TEXT NOT NULL,
    payload     TEXT,
    state       TEXT NOT NULL DEFAULT 'new'
                CHECK (state IN ('new', 'claimed', 'done', 'skipped', 'failed'))
);
CREATE INDEX ci_events_pending ON ci_events (received_at) WHERE state = 'new';
