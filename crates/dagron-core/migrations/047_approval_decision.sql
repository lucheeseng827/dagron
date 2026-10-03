-- Who decided an approval gate, and why. Both are NULL until the gate is
-- resolved; the decision instant is `finished_at`. A gate resolved by the
-- timeout sweep records `decided_by = 'timeout'`, so a decision nobody made
-- is distinguishable from a record that was never written.
ALTER TABLE task_runs ADD COLUMN decided_by TEXT;
ALTER TABLE task_runs ADD COLUMN decision_comment TEXT;
