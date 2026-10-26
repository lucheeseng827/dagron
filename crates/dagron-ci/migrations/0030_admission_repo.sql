-- Mirrors migrations_pg/0030_admission_repo.sql: the repository an admission
-- slot is for.
ALTER TABLE ci_admissions ADD COLUMN repo TEXT;
