-- Mirrors migrations_pg/0010_event_commit.sql: the commit an annotated tag's
-- event names. The comment there says what the column is for.
ALTER TABLE ci_events ADD COLUMN commit_sha TEXT;
