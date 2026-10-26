-- Mirrors migrations_pg/0013_event_association.sql: what the forge says a pull
-- request's author is to the repository. The comment there says what the
-- column is for.
ALTER TABLE ci_events ADD COLUMN association TEXT;
