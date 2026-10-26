-- The event queue by day: `ci_events` is partitioned on
-- `received_at`, a partition a day (UTC), so that the events of a day past
-- CI_RECORD_DAYS leave as one partition dropped, not as a DELETE.
-- Nothing is dropped unless that is set (`crate::retention`).
--
-- The table is made again as a partitioned one, and its rows copied in:
--   - its key is `(id, received_at)`, as a partitioned table's has to hold
--     the column it is partitioned on. `id` is a UUIDv7, made by dagron-ci
--     alone, and found through `ci_events_id`;
--   - `ci_api_requests` loses its foreign key to it, which a key of `id`
--     alone was needed for: a dropped day deletes its requests itself;
--   - a partition for each day that has events, for today and the seven
--     after it, and a default one, which catches an event of a day whose
--     partition was not made in time. dagron-ci makes the days ahead as it
--     runs.
--
-- It runs in one transaction, which holds `ci_events` locked from its
-- rename until it commits: the queue and the API wait for as long as the
-- copy takes, which on a large queue wants a maintenance window.
--
-- `payload_ref` names a payload kept in CI_OBJECT_STORE_URL, where it was
-- too large for `payload` (`crate::payloads`).
ALTER TABLE ci_api_requests DROP CONSTRAINT ci_api_requests_event_id_fkey;

ALTER TABLE ci_events RENAME TO ci_events_unpartitioned;
ALTER INDEX ci_events_pkey RENAME TO ci_events_unpartitioned_pkey;
ALTER INDEX ci_events_pending RENAME TO ci_events_unpartitioned_pending;
ALTER INDEX ci_events_claimed RENAME TO ci_events_unpartitioned_claimed;
ALTER INDEX ci_events_pr RENAME TO ci_events_unpartitioned_pr;
ALTER TABLE ci_events_unpartitioned ADD COLUMN payload_ref TEXT;

CREATE TABLE ci_events (LIKE ci_events_unpartitioned INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
    PARTITION BY RANGE (received_at);
ALTER TABLE ci_events ADD PRIMARY KEY (id, received_at);
ALTER TABLE ci_events ADD FOREIGN KEY (repo_id) REFERENCES ci_repos (id) ON DELETE CASCADE;
CREATE INDEX ci_events_id ON ci_events (id);
CREATE INDEX ci_events_pending ON ci_events (received_at) WHERE state = 'new';
CREATE INDEX ci_events_claimed ON ci_events (claim_expires_at) WHERE state = 'claimed';
CREATE INDEX ci_events_pr ON ci_events (repo_id, pr) WHERE pr IS NOT NULL;

DO $$
DECLARE
    today date := (now() AT TIME ZONE 'UTC')::date;
    day date;
BEGIN
    FOR day IN
        SELECT DISTINCT (received_at AT TIME ZONE 'UTC')::date FROM ci_events_unpartitioned
        UNION
        SELECT d::date FROM generate_series(today, today + 7, interval '1 day') AS d
    LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF ci_events FOR VALUES FROM (%L) TO (%L)',
            'ci_events_' || to_char(day, 'YYYYMMDD'),
            day::timestamp AT TIME ZONE 'UTC',
            (day + 1)::timestamp AT TIME ZONE 'UTC'
        );
    END LOOP;
END
$$;
CREATE TABLE ci_events_default PARTITION OF ci_events DEFAULT;

INSERT INTO ci_events SELECT * FROM ci_events_unpartitioned;
DROP TABLE ci_events_unpartitioned;
