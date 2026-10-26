-- 14.2: a token an agent acts with. See migrations_pg/086_agent_tokens.sql.
ALTER TABLE api_tokens ADD COLUMN agent TEXT;

CREATE TABLE IF NOT EXISTS agent_names (
    name       TEXT PRIMARY KEY NOT NULL,
    user_id    TEXT NOT NULL,
    created_at TEXT NOT NULL
);
