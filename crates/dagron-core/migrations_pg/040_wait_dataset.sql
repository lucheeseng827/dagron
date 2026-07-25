-- Dataset wait sensor (Dagster asset sensor). Parks like the time/HTTP sensors
-- (#27): running + NULL lease, `wait_dataset` names the dataset and
-- `wait_dataset_cursor` is the dataset_events.id high-water mark at park time —
-- only an update recorded after the park resolves the sensor. Mirrors
-- migrations/033_wait_dataset.sql. NULL = not a dataset sensor.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS wait_dataset TEXT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS wait_dataset_cursor BIGINT;
