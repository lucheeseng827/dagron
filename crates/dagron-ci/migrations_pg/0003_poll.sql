-- The poll sweep: what catches a commit no webhook announced. Each repository
-- is swept every `poll_secs` (NULL takes CI_POLL_SECS, 0 never polls it). A
-- sweep lists the repository's branches and tags with git's ref listing, and
-- its open pull requests from the forge's API, compares them with what the
-- last sweep saw, and queues an event per change. Those events carry the same
-- commit keys a webhook's would, so a commit both doors see runs once.
ALTER TABLE ci_repos ADD COLUMN poll_secs INTEGER;
-- When the repository is next due. NULL is due now: a repository just added.
ALTER TABLE ci_repos ADD COLUMN poll_due_at TIMESTAMPTZ;
-- The replica sweeping it, until the sweep ends or `poll_due_at` passes; a
-- sweep that ends after another replica took the repository over writes
-- nothing.
ALTER TABLE ci_repos ADD COLUMN poll_claim TEXT;
-- When a sweep last finished. NULL: never, and the first sweep records what
-- it finds as the baseline and queues nothing, so connecting a repository
-- does not run every branch it has.
ALTER TABLE ci_repos ADD COLUMN polled_at TIMESTAMPTZ;
-- The forge's ETag for the open pull requests, kept only while they fit on
-- one page: a 304 then says that none of them changed.
ALTER TABLE ci_repos ADD COLUMN poll_etag TEXT;
-- Why the last sweep failed; NULL once one succeeds.
ALTER TABLE ci_repos ADD COLUMN poll_error TEXT;
CREATE INDEX ci_repos_poll_due ON ci_repos (poll_due_at);

-- What the last sweep saw: each branch, tag and open pull request
-- (`refs/pull/{n}/head`) and the commit it pointed at.
CREATE TABLE ci_poll_refs (
    repo_id TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    ref     TEXT NOT NULL,
    sha     TEXT NOT NULL,
    PRIMARY KEY (repo_id, ref)
);
