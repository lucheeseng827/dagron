-- Fleet mode (plans 5.3 and 5.4).
-- How a runner's run was seen to end, where it was: its status (`succeeded`,
-- `failed`, `cancelled`), or `gone` where its cell had no such run. A
-- runner's `reason` is the first word of its end, and is often GitHub's (its
-- job ended); whether the runner died is its run's to say, whichever came
-- first.
ALTER TABLE ci_fleet_runners ADD COLUMN run_end TEXT;
-- When a GitLab project's Job Hook asked for a look while one was going: the
-- jobs it lists may be later than the look's, so another follows it. A
-- claim clears it.
ALTER TABLE ci_repos ADD COLUMN fleet_poked_at TIMESTAMPTZ;
