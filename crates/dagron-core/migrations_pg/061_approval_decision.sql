-- Who decided an approval gate, and why. Mirrors migrations/047_approval_decision.sql.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS decided_by TEXT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS decision_comment TEXT;
