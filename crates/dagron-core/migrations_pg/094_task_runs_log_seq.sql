-- A remote runner's log segment is applied once. A runner sends a task's
-- live log in numbered segments (`POST /runner/v1/tasks/{id}/logs/{seq}`) and
-- sends one again when it did not hear the answer. Where the first was
-- applied and only its answer was lost, the second appended the same text
-- again. dagron-api now keeps the last segment it applied, and the fence it
-- was applied under, and answers a segment it has applied already without
-- writing it.
--
-- Postgres's alone: runners dial dagron-api, which a SQLite engine never has,
-- and no engine statement reads these. Nullable columns with no default: no
-- table rewrite. NULL where no runner has sent a segment, or where one sent
-- before this migration did.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS log_seq BIGINT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS log_fence BIGINT;
