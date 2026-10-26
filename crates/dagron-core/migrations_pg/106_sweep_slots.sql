-- A slot for each of the reconcile loop's sweeps that are the installation's:
-- every engine ran every sweep on every tick, so with N engines each due row
-- was looked for N times. Now an engine whose own clock says a sweep is due
-- takes its slot, with the others it is due for, in one statement
-- (db::claim_sweep_slots), and runs it only where it took it: one engine a
-- window. `due_at` is when another may take it, `taken_at` when
-- it was taken, both the database's clock as the outbox's trigger writes it
-- (090). `turn` is one more at each take, and a write of `cursor` (the reap's
-- last run) or a hand-back holds only at the turn the slot was taken at.
--
-- Only a sweep that two engines may run at once is put on a slot: a slot is
-- not a lock, and an engine whose tick stalls after it took one may still be
-- sweeping when the next window goes to another. Schedules and cron stay on
-- the leadership lease (leader_election). An engine set SWEEP_SHARE=off, and
-- an engine from before this, takes no slot and sweeps every window itself.
--
-- No twin on SQLite: SQLite is one engine (dagron-api only opens the same
-- file), so every sweep is that engine's, and a write a tick for slots would
-- be one more write to the edge's flash for nothing.
CREATE TABLE IF NOT EXISTS sweep_slots (
    name     TEXT PRIMARY KEY,
    due_at   TEXT NOT NULL,
    holder   TEXT NOT NULL,
    turn     BIGINT NOT NULL DEFAULT 0,
    cursor   TEXT,
    taken_at TEXT NOT NULL
);

-- The HTTP sensors' read no longer takes `next_poll_at IS NULL` for due: a
-- sensor is parked with it set (db::park_wait_url), and the branch made the
-- read take every parked sensor. One parked without it by some older path is
-- set due now; 1970 is before any time, in any of the formats it is kept in.
UPDATE task_runs SET next_poll_at = '1970-01-01T00:00:00+00:00'
 WHERE status = 'running' AND wait_url IS NOT NULL AND next_poll_at IS NULL;
