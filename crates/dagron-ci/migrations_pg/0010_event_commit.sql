-- The commit an event's id names, where the id is not a commit's own: an
-- annotated tag's event carries the tag object's id, which stays `sha` and the
-- commit key, since a webhook and a poll sweep must derive one key for it.
-- What runs, is checked out, is given a credential and is reported on is this
-- commit. NULL for every other event: `sha` is the commit.
ALTER TABLE ci_events ADD COLUMN commit_sha TEXT;
