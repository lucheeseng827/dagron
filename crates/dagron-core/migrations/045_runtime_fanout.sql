-- Fan-out over an upstream task's output, resolved at run time
-- (mirrors migrations_pg/057_runtime_fanout.sql + 058 for its index).
--
-- `with_items:` / `with_param:` are resolved by the expander before the run
-- exists, which is what lets `budget:` refuse a blow-up at submit. A fan-out
-- over a *result* cannot be: at expansion time the producer has not run.
--
-- So the task is created as ONE row that never executes. When its dependencies
-- are satisfied it parks — `status = 'running'`, claim and lease NULL, the same
-- shape the sub-workflow trigger and the wait sensors already use — and the
-- reconcile sweep reads the producer's output, parses a JSON array, and inserts
-- one instance row per element.
--
-- The parked row stays as the **join point**. That is the design decision worth
-- naming: the alternative is rewiring every dependent onto the new instances
-- mid-run, which means editing edges under a scheduler that is concurrently
-- reading them. Keeping the barrier means dependents are already wired to the
-- thing they should wait for, `trigger_rule` and `allow_failure` keep their
-- meaning, and the sweep's only edge writes are new ones.

-- On the barrier: the producer task's authored NAME (not an id — the row is
-- written before ids are resolvable to names). NULL on every ordinary task,
-- which is what keeps this out of the claim path's way entirely: a barrier is
-- never `ready`, so `claim_ready` needs no new predicate.
ALTER TABLE task_runs ADD COLUMN fanout_of TEXT;

-- On an instance: the barrier's task id. The sweep's second phase reads these
-- to decide whether the barrier is done, rather than leaning on
-- `remaining_deps` — the barrier is parked `running`, and the dependent
-- decrement only touches `pending` rows.
ALTER TABLE task_runs ADD COLUMN fanout_parent TEXT;

-- Partial: `fanout_parent` is NULL for every row in a workflow that does not
-- use this, so the index costs those runs nothing.
CREATE INDEX IF NOT EXISTS idx_task_runs_fanout_parent
    ON task_runs(fanout_parent) WHERE fanout_parent IS NOT NULL;
