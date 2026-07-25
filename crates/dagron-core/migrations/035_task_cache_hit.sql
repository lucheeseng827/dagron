-- Mark a task row that was resolved from the memoization store (#22) rather
-- than executed. A cache hit is otherwise indistinguishable from a real run:
-- the row is `succeeded` with the reused output, so the console cannot say "this
-- took 0s because it was cached" and an operator reading a suspiciously fast run
-- has nothing to go on. The engine previously recorded a hit only as a log line
-- and a Prometheus counter — neither of which is attached to the row it explains.
--
-- Default 0 = executed normally, so every existing row keeps its meaning.
ALTER TABLE task_runs ADD COLUMN cache_hit INTEGER NOT NULL DEFAULT 0;
