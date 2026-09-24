-- Git-managed workflows: which connected repo (git_repos.id) a workflow was synced from, and the
-- md5 of the spec exactly as git delivered it. `md5(spec) <> managed_md5` is what "drifted" means
-- (a console edit since the last sync). dagron-api and dagron-gitops own the writes; dagron-api also
-- ensures these columns at startup (ALTER ... IF NOT EXISTS), like `description`, so it works
-- without an engine redeploy.
ALTER TABLE IF EXISTS workflows ADD COLUMN IF NOT EXISTS managed_by TEXT;
ALTER TABLE IF EXISTS workflows ADD COLUMN IF NOT EXISTS managed_md5 TEXT;
