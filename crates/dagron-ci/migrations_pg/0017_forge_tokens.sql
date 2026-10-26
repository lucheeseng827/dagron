-- A forge token a job holds (a workflow's GITHUB_TOKEN): minted by the same
-- service as a checkout's credential, and recorded in the same table, with
-- the permissions it was given. NULL for a checkout's credential, which is
-- contents: read and reaches the fetch alone.
ALTER TABLE ci_checkout_credentials ADD COLUMN permissions TEXT;
