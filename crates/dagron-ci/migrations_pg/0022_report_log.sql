-- What dagron-ci told the forge, and where its records went.
--
-- `ci_report_log`: a row for each status dagron-ci posted on a commit, as
-- it posted it, and what came of it: `posted`, or `refused`, `rate_limited`
-- or `failed` with why. Of an event always, and of a run where it was a
-- run's job. Written as the post is answered, and never updated: what was
-- posted when, which `ci_runs.reported` (the last state of each status) does
-- not keep. It goes with its event's day.
CREATE TABLE ci_report_log (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL,
    run_id      TEXT,
    repo_id     TEXT NOT NULL,
    sha         TEXT NOT NULL,
    context     TEXT NOT NULL,
    state       TEXT NOT NULL,
    description TEXT,
    target_url  TEXT,
    posted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome     TEXT NOT NULL CHECK (outcome IN ('posted', 'refused', 'rate_limited', 'failed')),
    detail      TEXT
);
CREATE INDEX ci_report_log_event ON ci_report_log (event_id, id);

-- `ci_segments`: a day's rows of one table, written to CI_OBJECT_STORE_URL
-- before they leave the database (`crate::records`): zstd-compressed JSON,
-- a row a line. A segment holds the rows of one retention scope; there is
-- one, `instance`, until a repository or an organisation has a policy of its
-- own. Its rows leave the database only once it was read back and its
-- checksum matched (`verified_at`), and it is deleted at `expires_at`
-- (CI_SEGMENT_DAYS after its day; NULL keeps it).
CREATE TABLE ci_segments (
    table_name  TEXT NOT NULL,
    day         TEXT NOT NULL,
    scope       TEXT NOT NULL,
    part        INTEGER NOT NULL DEFAULT 0,
    object_key  TEXT NOT NULL,
    row_count   BIGINT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    sha256      TEXT NOT NULL,
    -- The SHA-256 of the table's rows of the day as they were read, in
    -- their order: what a segment is reused for, and a day dropped by, only
    -- where the rows still are.
    rows_sha256 TEXT NOT NULL DEFAULT '',
    written_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ,
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
