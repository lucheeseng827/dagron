-- A repository is disconnected, and no longer deleted: the events it had and
-- the runs they made stay to be read, which a deleted row took with it.
-- NULL while it is connected. A repository that is connected again is the
-- same row, and its history goes on.
ALTER TABLE ci_repos ADD COLUMN disconnected_at TIMESTAMPTZ;

-- Who connected, changed and disconnected a repository, and when: one row a
-- change, written with it. `actor` is the caller as dagron-api named them
-- (their email, or their subject where they have none), or `cli:` and the
-- operator's name for a change made with the database's own credentials.
-- `detail` is what the change set, as JSON.
CREATE TABLE ci_repo_changes (
    id      TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL
            CHECK (action IN ('connected', 'reconnected', 'changed', 'disconnected')),
    detail  TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX ci_repo_changes_repo ON ci_repo_changes (repo_id, at);
