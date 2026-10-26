-- 14.2: a token an agent acts with. `agent` names it (`agent:<name>` is the
-- caller such a token presents); NULL is a person's own token, as before.
ALTER TABLE api_tokens ADD COLUMN IF NOT EXISTS agent TEXT;

-- Whose each agent's name is, for good. Its runs are recorded as
-- `agent:<name>`, and are what it may rerun and cancel: a name that passed to
-- another person once its tokens were revoked would pass them its runs too.
CREATE TABLE IF NOT EXISTS agent_names (
    name       TEXT PRIMARY KEY NOT NULL,
    user_id    TEXT NOT NULL,
    created_at TEXT NOT NULL
);
