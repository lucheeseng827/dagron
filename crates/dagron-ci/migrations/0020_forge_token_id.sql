-- Mirrors migrations_pg/0020_forge_token_id.sql: the forge's own id of a
-- credential it revokes by id (a GitLab project access token), NULL otherwise.
ALTER TABLE ci_checkout_credentials ADD COLUMN forge_token_id TEXT;
