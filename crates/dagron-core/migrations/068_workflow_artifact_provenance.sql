-- 8.7: signed provenance of each workflow artifact. See
-- migrations_pg/093_workflow_artifact_provenance.sql.
ALTER TABLE workflow_artifacts ADD COLUMN provenance TEXT;
