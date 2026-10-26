-- A task's log out of the scheduling database. A task
-- attempt's log is closed segments, each a zstd object of the log store
-- (DAGRON_LOG_STORE), and the open part still being written; a row here for
-- each closed segment, in order. Its task row's `log` keeps only the open
-- part (an object store), or nothing but notes (a file:// store). Written
-- once a segment closes: about one row for each 8 MiB of log.
--
-- A segment is its attempt's (the fence it was written under): a reader
-- takes the segments of the task's current attempt alone. Gone with its
-- task row; its object is the log store's to remove.
CREATE TABLE IF NOT EXISTS log_segments (
    task_id     TEXT NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    fence       INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    object_key  TEXT NOT NULL,
    bytes       INTEGER NOT NULL,
    lines       INTEGER NOT NULL,
    closed_at   TEXT NOT NULL,
    PRIMARY KEY (task_id, fence, seq)
);
