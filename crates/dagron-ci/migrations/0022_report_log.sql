-- What dagron-ci told the forge, and where its records went,
-- as the Postgres migration of the same number says.
CREATE TABLE ci_report_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    run_id      TEXT,
    repo_id     TEXT NOT NULL,
    sha         TEXT NOT NULL,
    context     TEXT NOT NULL,
    state       TEXT NOT NULL,
    description TEXT,
    target_url  TEXT,
    posted_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    outcome     TEXT NOT NULL CHECK (outcome IN ('posted', 'refused', 'rate_limited', 'failed')),
    detail      TEXT
);
CREATE INDEX ci_report_log_event ON ci_report_log (event_id, id);

CREATE TABLE ci_segments (
    table_name  TEXT NOT NULL,
    day         TEXT NOT NULL,
    scope       TEXT NOT NULL,
    part        INTEGER NOT NULL DEFAULT 0,
    object_key  TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    size_bytes  INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    rows_sha256 TEXT NOT NULL DEFAULT '',
    written_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    verified_at TEXT,
    expires_at  TEXT,
    PRIMARY KEY (table_name, day, scope, part)
);

-- `ci_archived`: each event and engine run of a day that left the
-- database once it was written out, by its id: the event it is of, its day,
-- and its repository's cell, whose dagron-api says who may read it. What
-- was posted for it is read from that day's segments, by the reader that
-- cell knows. Its rows go once their day has no segment left.
CREATE TABLE ci_archived (
    id       TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    day      TEXT NOT NULL,
    cell     TEXT NOT NULL
);
CREATE INDEX ci_archived_day ON ci_archived (day);
