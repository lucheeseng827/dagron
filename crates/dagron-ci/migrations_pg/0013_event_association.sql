-- What the forge says a pull request's author is to the repository, as it
-- said it: CONTRIBUTOR, FIRST_TIME_CONTRIBUTOR, NONE and the rest. The event's
-- trust was decided from it where the event was read, and it was dropped.
-- Whether an untrusted pull request is its author's first is read off it when
-- the event is handled: such a one waits for an approval before anything of
-- it is run. NULL for an event that is no pull request's, for one the forge
-- said nothing of, and for every event from before this column.
ALTER TABLE ci_events ADD COLUMN association TEXT;
