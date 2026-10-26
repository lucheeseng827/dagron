-- Mirrors migrations_pg/099_environment_policy_agents.sql: an identity for an
-- agent's run, and a protected environment's approvers. The comment
-- there says why.
ALTER TABLE environment_policies ADD COLUMN allowed_identity TEXT;
ALTER TABLE environment_policies ADD COLUMN approvers TEXT;
