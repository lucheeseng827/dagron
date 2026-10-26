-- The event queue by day, as SQLite has it: no partitions, which SQLite has none of. Events
-- past CI_RECORD_DAYS are deleted where that is set
-- (`crate::retention`). `payload_ref` names a payload kept in
-- CI_OBJECT_STORE_URL, where it was too large for `payload`
-- (`crate::payloads`).
ALTER TABLE ci_events ADD COLUMN payload_ref TEXT;
