-- How a repository is served.
--   native  its `.dagron/ci` pipelines, which dagron-ci runs
--   compat  those, and the forge's own files its `.dagron/ci.yaml` names
--           (`.github/workflows`, `.gitlab-ci.yml`), which dagron-ci runs too
--   fleet   none: its forge runs its pipelines, and dagron-ci the runners of
--           the jobs that ask for its labels
-- A repository connected before this is `compat`, which is what it was served
-- as: the forge's files were read wherever the build read them. One connected
-- after is `native` unless it is said otherwise.
ALTER TABLE ci_repos ADD COLUMN mode TEXT NOT NULL DEFAULT 'native'
    CHECK (mode IN ('native', 'compat', 'fleet'));
UPDATE ci_repos SET mode = 'compat';
