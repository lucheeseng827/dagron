-- Optional human description for a first-class workflow (shown under the name in
-- the Workflows table/board). UI-owned; the engine doesn't read it. dagron-api
-- also ensures this column at startup (ALTER ... IF NOT EXISTS) so it works
-- without an engine redeploy.
ALTER TABLE IF EXISTS workflows ADD COLUMN IF NOT EXISTS description TEXT;
