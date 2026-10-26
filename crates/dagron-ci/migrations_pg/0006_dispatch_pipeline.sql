-- A dispatch may name the one pipeline it runs: a file in `.dagron/ci`, as
-- the run API's caller named it (`deploy` or `deploy.yaml`). NULL runs every
-- pipeline whose `on:` takes the event, and is the only value other events
-- have.
ALTER TABLE ci_events ADD COLUMN pipeline TEXT;
