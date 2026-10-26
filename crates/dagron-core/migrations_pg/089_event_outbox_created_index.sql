-- no-transaction
-- 10.6: a sink enrols the outbox's events by when they were written, from its
-- cursor (088_outbox_deliveries.sql). CONCURRENTLY in its own migration, like
-- 065: the outbox is append-only and can be large. An interrupted build can
-- leave an INVALID index that IF NOT EXISTS skips — drop it and re-run
-- migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_outbox_created
    ON event_outbox (created_at);
