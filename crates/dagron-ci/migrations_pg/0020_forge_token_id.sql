-- A GitLab project access token, minted for one checkout, is revoked by its
-- id, with the operator's token for the host: the id is kept with its row.
-- NULL for a GitHub installation token, which revokes itself.
ALTER TABLE ci_checkout_credentials ADD COLUMN forge_token_id TEXT;
