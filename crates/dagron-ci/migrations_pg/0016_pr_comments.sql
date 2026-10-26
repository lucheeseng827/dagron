-- A pull request's summary comment (CI_GITHUB_PR_COMMENT=on): the one comment
-- dagron-ci keeps on a pull request, which says how the runs of its latest
-- commit went and is written again, in place, as they change.
--
-- It has one writer at a time, as a run has one reporter. A writer claims the
-- row (`claim`, with `due_at` as the end of its lease) and leaves it by its
-- claim. A run that changes meanwhile does not take the row from it: it sets
-- `again`, and the writer leaves the row due at once, to be written from
-- what the runs then say.
CREATE TABLE ci_pr_comments (
    repo_id    TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    pr         BIGINT NOT NULL,
    -- The forge's id for the comment, once there is one.
    comment_id TEXT,
    -- A digest of what the comment says: a write that would say the same is
    -- not made.
    said       TEXT,
    -- When it is to be written. NULL once it says what there is to say.
    due_at     TIMESTAMPTZ,
    -- When it was last written. It is not written again for a while after:
    -- a run's jobs change one after another, and a comment written for each
    -- would be dozens of edits of one comment.
    written_at TIMESTAMPTZ,
    claim      TEXT,
    again      BOOLEAN NOT NULL DEFAULT FALSE,
    -- Why it was last not written.
    error      TEXT,
    PRIMARY KEY (repo_id, pr)
);
CREATE INDEX ci_pr_comments_due ON ci_pr_comments (due_at) WHERE due_at IS NOT NULL;

-- What a run's jobs came to as the reporter last saw them, as JSON: what the
-- comment is written from. NULL for a run no comment is kept of.
ALTER TABLE ci_runs ADD COLUMN jobs TEXT;

-- A pull request's events, by which its comment finds its runs.
CREATE INDEX ci_events_pr ON ci_events (repo_id, pr) WHERE pr IS NOT NULL;
