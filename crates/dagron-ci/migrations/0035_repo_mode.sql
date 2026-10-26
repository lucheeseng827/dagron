-- Mirrors migrations_pg/0035_repo_mode.sql: how a repository is served
-- (`native`, `compat` or `fleet`). The comments there say what each is.
ALTER TABLE ci_repos ADD COLUMN mode TEXT NOT NULL DEFAULT 'native'
    CHECK (mode IN ('native', 'compat', 'fleet'));
UPDATE ci_repos SET mode = 'compat';
