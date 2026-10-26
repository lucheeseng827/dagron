-- The HTTP sensors' read no longer takes `next_poll_at IS NULL` for due, so
-- that it reads idx_task_runs_wait_url_due (076) for the sensors due alone:
-- a sensor is parked with it set (db::park_wait_url), and the branch made
-- the read take every parked sensor. One parked without it by some older
-- path is set due now; 1970 is before any time, in any of the formats it is
-- kept in. As migrations_pg/106 does.
--
-- There is no table of sweep slots here, as there is on Postgres: SQLite is
-- one engine, and every sweep is its own.
UPDATE task_runs SET next_poll_at = '1970-01-01T00:00:00+00:00'
 WHERE status = 'running' AND wait_url IS NOT NULL AND next_poll_at IS NULL;
