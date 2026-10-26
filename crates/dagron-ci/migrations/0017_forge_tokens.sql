-- Mirrors migrations_pg/0017_forge_tokens.sql: the permissions a forge token
-- a job holds was given, NULL for a checkout's credential.
ALTER TABLE ci_checkout_credentials ADD COLUMN permissions TEXT;
