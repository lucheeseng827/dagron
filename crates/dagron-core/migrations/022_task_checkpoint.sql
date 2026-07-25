-- Checkpoint-aware resume for long tasks (training jobs, stream consumers).
-- A running task may report the artifact it checkpointed to (plus an optional
-- step/epoch marker); the pointer is carried across retries so the next attempt
-- is dispatched with DAGRON_RESUME_FROM instead of restarting from zero. dagron
-- owns pointer durability only — what is inside the checkpoint belongs to the
-- task (PyTorch/JAX/consumer offsets/…). NULL = no checkpoint reported, the
-- pre-migration behaviour for every existing row.
ALTER TABLE task_runs ADD COLUMN checkpoint_uri TEXT;
ALTER TABLE task_runs ADD COLUMN checkpoint_marker TEXT;
