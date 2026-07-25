-- Dataset wait sensor (Dagster asset sensor). A `type: wait` task with a
-- `wait.dataset` parks like the time/HTTP sensors (#27): `running` with
-- `claimed_by`/`lease_expires_at` NULL, so the claim scan skips it and lease
-- recovery leaves it alone — no new status value. `wait_dataset` names the
-- dataset; `wait_dataset_cursor` is the dataset_events.id high-water mark at
-- park time, so only an update recorded *after* the park resolves the sensor
-- (fresh data, not any data). NULL = not a dataset sensor.
ALTER TABLE task_runs ADD COLUMN wait_dataset TEXT;
ALTER TABLE task_runs ADD COLUMN wait_dataset_cursor BIGINT;
