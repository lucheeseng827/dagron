-- Workers run a repository's own pipelines, `.dagron/ci/*.yaml` read at the
-- event's commit, and a pipeline's `on:` decides by the event.
--
-- The branch a pull request or merge group merges into, as the forge names it
-- (`main`, or `refs/heads/main`): `on.pull_request.branches` and
-- `on.merge_group.branches` filter on it. NULL for every other event.
ALTER TABLE ci_events ADD COLUMN base_ref TEXT;
