-- 8.7: signed provenance of each workflow artifact. Where the engine is given
-- a key to sign with (DAGRON_PROVENANCE_KEY, or a KMS key in the enterprise
-- build), an artifact becomes ready with its SLSA v1 provenance: a DSSE
-- envelope, as `cosign verify-blob-attestation` reads one, written in the
-- same statement that makes the artifact its run's. NULL where none was
-- signed. A nullable column with no default: no table rewrite.
ALTER TABLE workflow_artifacts ADD COLUMN IF NOT EXISTS provenance TEXT;
