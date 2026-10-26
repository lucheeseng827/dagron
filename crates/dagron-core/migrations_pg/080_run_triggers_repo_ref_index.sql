-- no-transaction
-- A repository's runs, by ref.
--
-- `run_triggers` (072) says which repository, ref and commit a run was for.
-- Until now it was read by its run alone. The gateway now answers a question
-- of it that a console's repository page asks: a repository's refs, each
-- with the runs for its latest commit (`GET /api/runs/refs`). This index is
-- what keeps that from reading the whole table: a repository's rows, in the
-- order of its refs, the latest first.
--
-- Postgres only: the gateway that asks runs on Postgres. The SQLite table
-- (migrations/057) is read by its run alone, as before.
--
-- CONCURRENTLY in its own single-statement migration, like 071, 074 and 075,
-- so building it never blocks a run's creation, which writes this table. An
-- interrupted build can leave an INVALID index that IF NOT EXISTS skips; drop
-- it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_run_triggers_repo_ref
    ON run_triggers (forge_host, repo, ref, created_at DESC);
