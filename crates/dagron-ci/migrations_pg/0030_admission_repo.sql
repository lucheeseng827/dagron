-- Which repository an admission slot is for: a build that holds
-- teams to shares of its limit counts the runs being submitted by
-- repository, as it counts those submitted (ci_runs.repo_id). A slot taken
-- before this column existed has none, and counts in the total alone.
ALTER TABLE ci_admissions ADD COLUMN repo TEXT;
