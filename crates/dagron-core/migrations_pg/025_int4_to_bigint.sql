-- Fix: two Postgres columns were declared INTEGER (INT4) but the Rust code decodes them as i64
-- (INT8), which sqlx rejects at runtime: "mismatched types; Rust type i64 (as SQL type INT8) is
-- not compatible with SQL type INT4". Every other numeric column in migrations_pg is BIGINT by
-- design; these two slipped through as INTEGER.
--
--   * task_runs.is_approval  (021_approval.sql)  — read as i64 in advance_ready_tasks(); this one
--     crash-loops the engine, because that query runs on every reconcile tick and fails the moment
--     any task is pending, blocking all DAG execution on Postgres.
--   * event_outbox.attempts  (011_event_outbox.sql) — read as i64 in the outbox delivery loop
--     (row.try_get("attempts")), so it crashes as soon as an outbox event is dispatched.
--
-- Widening INT4 -> INT8 is a lossless in-place ALTER (no rewrite of the data value), safe on a live
-- table. Postgres-only migration: the SQLite path uses the separate ./migrations dir where dynamic
-- typing makes INTEGER decode as i64 already, so it is unaffected.
ALTER TABLE task_runs    ALTER COLUMN is_approval TYPE BIGINT;
ALTER TABLE event_outbox ALTER COLUMN attempts    TYPE BIGINT;
