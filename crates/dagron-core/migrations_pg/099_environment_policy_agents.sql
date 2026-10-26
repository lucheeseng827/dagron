-- What an environment gives a run that is not trusted,
-- beyond its secrets. `allowed_identity`: the classes besides `trusted`
-- whose tasks in it may hold an identity (OIDC), as JSON, `["agent"]` at
-- most. `approvers`: a protected environment, as JSON: who must approve
-- such a run's use of it before the task that names it runs. NULL is none
-- of either, so every policy stored before this reads as it did.
ALTER TABLE environment_policies ADD COLUMN IF NOT EXISTS allowed_identity TEXT;
ALTER TABLE environment_policies ADD COLUMN IF NOT EXISTS approvers TEXT;
