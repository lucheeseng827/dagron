-- A slot keeps its run's caller, the actor its
-- event names, so a build can count each caller's runs (`Admission::by_caller`).
-- A run's caller is its event's; a slot taken before this has none.
ALTER TABLE ci_admissions ADD COLUMN actor TEXT;
