-- Mirrors migrations_pg/0008_admission.sql: a slot per run between admission
-- and its record in ci_runs. The comments there say what each part is for.
CREATE TABLE ci_admissions (
    idempotency_key TEXT PRIMARY KEY,
    expires_at      TEXT NOT NULL
);
CREATE INDEX ci_runs_active ON ci_runs (run_id)
    WHERE status = 'submitted' AND report_due_at IS NOT NULL;
