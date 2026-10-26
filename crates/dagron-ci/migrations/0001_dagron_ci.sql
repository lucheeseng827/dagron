-- Mirrors migrations_pg/0001_dagron_ci.sql: the same tables, with timestamps as
-- RFC 3339 text. The comments there say what each table is for.

CREATE TABLE ci_repos (
    id         TEXT PRIMARY KEY,
    forge      TEXT NOT NULL
               CHECK (forge IN ('github', 'gitlab', 'gitea', 'forgejo', 'bitbucket', 'generic')),
    forge_host TEXT NOT NULL,
    full_name  TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (forge_host, full_name)
);

CREATE TABLE ci_deliveries (
    forge_host  TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (forge_host, delivery_id)
);

CREATE TABLE ci_events (
    id          TEXT PRIMARY KEY,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    repo_id     TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    source      TEXT NOT NULL CHECK (source IN ('webhook', 'poll', 'api', 'relay')),
    delivery_id TEXT,
    kind        TEXT NOT NULL,
    ref         TEXT NOT NULL,
    sha         TEXT NOT NULL,
    base_sha    TEXT,
    pr          INTEGER,
    actor       TEXT,
    trust       TEXT NOT NULL CHECK (trust IN ('trusted', 'untrusted', 'agent')),
    commit_key  TEXT NOT NULL,
    payload     TEXT,
    state       TEXT NOT NULL DEFAULT 'new'
                CHECK (state IN ('new', 'claimed', 'done', 'skipped', 'failed'))
);
CREATE INDEX ci_events_pending ON ci_events (received_at) WHERE state = 'new';
